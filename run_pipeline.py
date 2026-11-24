import os
import shlex
import subprocess
import sys
import time
from pathlib import Path

# Repo root. The Airflow Dag and this script both run from here so relative
# paths (dev.duckdb, profiles.yml, dbt_project.yml) resolve the same way.
ROOT = Path(__file__).resolve().parent


def _dbt_executable() -> str:
    """dbt is installed next to the interpreter in the project virtualenv."""
    scripts = Path(sys.executable).parent
    for name in ("dbt", "dbt.exe"):
        candidate = scripts / name
        if candidate.exists():
            return str(candidate)
    return "dbt"


def run_step(command: str, description: str):
    """מריץ פקודת טרמינל עם מדידת זמנים ועצירה במקרה של שגיאה (check=True)."""
    print(f"\n==========================================")
    print(f"🚀 Starting Step: {description}")
    print(f"==========================================")
    start_time = time.time()
    
    try:
        subprocess.run(command, shell=True, check=True)
        elapsed = time.time() - start_time
        print(f"✅ Finished: {description} (took {elapsed:.2f}s)")
    except subprocess.CalledProcessError as e:
        print(f"\n🚨 ERROR: Step '{description}' failed with exit code {e.returncode}!")
        print("Stopping execution of downstream pipeline steps.")
        sys.exit(e.returncode)

if __name__ == "__main__":
    # Local fallback for dags/enterprise_data_pipeline.py. Same order, one process.
    # dbt reads ~/.dbt/profiles.yml unless this directory is set. The profile in
    # the repo is a local DuckDB file and has no secrets.
    os.chdir(ROOT)
    os.environ.setdefault("DBT_PROFILES_DIR", str(ROOT))

    python = shlex.quote(sys.executable)
    dbt = shlex.quote(_dbt_executable())
    profile_flags = "--project-dir . --profiles-dir ."

    total_start = time.time()
    print("------------------------------------------")
    print("🏁 Starting Full Enterprise Data Pipeline")
    print("------------------------------------------")
    
    # 1. הרצת ה-Ingestion הישן (תגובות)
    run_step(f"{python} ingest_comments.py", "Legacy Ingestion (Comments)")
    
    # 2. הרצת ה-Ingestion הרסיליאנטי החדש (API)
    run_step(f"{python} ingest_api_resilient.py", "Resilient API Ingestion (Pagination & Retries)")

    # 3. SCD2 snapshot. `dbt run` builds models only, so the snapshot is its own step.
    #    It reads raw_users, which step 1 just loaded.
    run_step(f"{dbt} snapshot {profile_flags}", "dbt SCD Type 2 Snapshot (snap_users)")
    
    # 4. הרצת מודלי ה-dbt (Staging + Marts)
    run_step(f"{dbt} run {profile_flags}", "dbt Model Transformations")
    
    # 5. הרצת בדיקות האיכות ב-dbt
    run_step(f"{dbt} test {profile_flags}", "dbt Data Quality Tests")
    
    total_elapsed = time.time() - total_start
    print(f"\n🎉 FULL PIPELINE COMPLETED SUCCESSFULLY in {total_elapsed:.2f}s!")