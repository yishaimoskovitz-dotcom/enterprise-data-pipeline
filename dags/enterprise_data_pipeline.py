"""Enterprise data pipeline.

Same work as ``run_pipeline.py``, with Airflow owning the schedule, the
retries, and the order. The Python ingest scripts and the dbt project stay
where they are. This file only decides when to call them.

Order (each step writes the same DuckDB file, so the steps are serial):

1. ``ingest_comments.py`` loads users and comments.
2. ``ingest_api_resilient.py`` loads posts.
3. ``dbt snapshot`` records SCD2 history for users (``snap_users``).
4. ``dbt run`` builds staging views and mart tables.
5. ``dbt test`` checks the mart contracts.

``dbt run`` does not include snapshots. dbt keeps those as a separate command
so a full model rebuild does not rewrite history. The snapshot has to be
called on its own, after the raw users table exists.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

# Dag files live in <repo>/dags. The project root is the parent of that folder
# when you run `airflow standalone` from a checkout. Docker Compose mounts the
# repo at a different path and sets PIPELINE_PROJECT_DIR, which wins here.
PROJECT_DIR = Path(
    os.environ.get("PIPELINE_PROJECT_DIR", Path(__file__).resolve().parent.parent)
).resolve()

# Shared by every task. append_env=True keeps the container/venv PATH, so
# `python` and `dbt` still resolve. Setting env without append_env would
# replace the environment and the commands would fail with "python: not found".
TASK_ENV = {"PIPELINE_PROJECT_DIR": str(PROJECT_DIR)}

# JSONPlaceholder is a small public API. These bounds are short enough that a
# retry is visible during a demo, and long enough that a blip does not turn
# into a tight request loop. The posts script also retries inside the process.
INGEST_RETRY_DELAY = timedelta(seconds=30)
DBT_RETRY_DELAY = timedelta(seconds=30)


def project_bash(task_id: str, command: str, **kwargs) -> BashOperator:
    """Run a command with the repo as the working directory.

    The ingest scripts open ``dev.duckdb`` relative to the current directory.
    dbt is pointed at this repo with explicit flags as well, because dbt's
    default profile directory is ``~/.dbt``, not the project folder. The
    profile in this repo has no secrets. It is a local DuckDB file.
    """
    return BashOperator(
        task_id=task_id,
        bash_command=f'cd "$PIPELINE_PROJECT_DIR" && {command}',
        env=TASK_ENV,
        append_env=True,
        **kwargs,
    )


with DAG(
    dag_id="enterprise_data_pipeline",
    description="Ingest JSONPlaceholder into DuckDB, snapshot users, then build and test dbt models.",
    # Anchor the daily schedule. Timezone-aware so the run does not shift with
    # the machine's local timezone. @daily is 00:00 UTC.
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule="@daily",
    # The API serves the current snapshot of the data, not a date partition.
    # Catchup would reload that same snapshot once per missed day.
    catchup=False,
    # A second run overlapping the first would try to write dev.duckdb while
    # it is already locked. One active run, plus the serial chain below, keeps
    # a single writer.
    max_active_runs=1,
    # A fresh metadata database should not call the API until someone unpauses
    # the Dag in the UI (or with `airflow dags unpause`).
    is_paused_upon_creation=True,
    default_args={
        "owner": "data-engineering",
        # Yesterday's failure should not block today's load. Each run replaces
        # the raw tables with the current API response.
        "depends_on_past": False,
        "email_on_failure": False,
        "retries": 1,
        "retry_delay": DBT_RETRY_DELAY,
    },
    tags=["portfolio", "dbt", "duckdb"],
    doc_md=__doc__,
) as dag:
    # BashOperator, not a Python import of the scripts. ingest_comments.py
    # runs its HTTP calls at import time, so importing it while Airflow parses
    # this file would hit the API on every Dag refresh. A shell command stays
    # side-effect free until the task actually runs.
    #
    # Retries sit outside the script. ingest_comments.py has no retry loop of
    # its own. The posts script retries a single page, then raises if the page
    # still failed. An Airflow retry restarts the whole load. That is safe:
    # both scripts use CREATE OR REPLACE, so a second attempt overwrites the
    # raw tables instead of appending duplicate rows.
    #
    # retry_exponential_backoff spaces those restarts (about 30s, 60s, 120s,
    # capped at 5 minutes) so a down API is not polled in a tight loop.
    ingest_comments = project_bash(
        "ingest_comments",
        "python ingest_comments.py",
        retries=3,
        retry_delay=INGEST_RETRY_DELAY,
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=10),
    )
    ingest_api_posts = project_bash(
        "ingest_api_posts",
        "python ingest_api_resilient.py",
        retries=3,
        retry_delay=INGEST_RETRY_DELAY,
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=10),
    )

    # The two loads write different tables (raw_users, raw_comments, raw_api_posts)
    # but the same DuckDB file. DuckDB allows one writer, so these tasks are
    # ordered even though their tables do not depend on each other. On a
    # server warehouse they could run in parallel.
    #
    # Comments are first because the snapshot reads raw_users, which that
    # script creates. Posts have no consumer until `dbt run`.
    dbt_snapshot = project_bash(
        "dbt_snapshot",
        # --project-dir selects dbt_project.yml. --profiles-dir selects the
        # profiles.yml next to it. Without the second flag, dbt looks in ~/.dbt
        # and fails on a clean machine.
        #
        # One retry covers a DuckDB file lock left by the ingest step. The
        # snapshot itself is safe to repeat: strategy=check compares email and
        # name, and writes a new row only when one of those columns changed.
        'dbt snapshot --project-dir "$PIPELINE_PROJECT_DIR" --profiles-dir "$PIPELINE_PROJECT_DIR"',
        retries=1,
        retry_delay=DBT_RETRY_DELAY,
        execution_timeout=timedelta(minutes=10),
    )
    # Models are views and tables, not incremental, so a retry rebuilds them.
    dbt_run = project_bash(
        "dbt_run",
        'dbt run --project-dir "$PIPELINE_PROJECT_DIR" --profiles-dir "$PIPELINE_PROJECT_DIR"',
        retries=1,
        retry_delay=DBT_RETRY_DELAY,
        execution_timeout=timedelta(minutes=10),
    )
    # A failed test is a data-quality result, not a transient error. Retrying
    # it would run the same assertions again. `dbt test` only reads.
    dbt_test = project_bash(
        "dbt_test",
        'dbt test --project-dir "$PIPELINE_PROJECT_DIR" --profiles-dir "$PIPELINE_PROJECT_DIR"',
        retries=0,
        execution_timeout=timedelta(minutes=10),
    )

    ingest_comments >> ingest_api_posts >> dbt_snapshot >> dbt_run >> dbt_test
