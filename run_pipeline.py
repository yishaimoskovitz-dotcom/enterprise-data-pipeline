import subprocess
import sys
import time

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
    total_start = time.time()
    print("------------------------------------------")
    print("🏁 Starting Full Enterprise Data Pipeline")
    print("------------------------------------------")
    
    # 1. הרצת ה-Ingestion הישן (תגובות)
    run_step("python ingest_comments.py", "Legacy Ingestion (Comments)")
    
    # 2. הרצת ה-Ingestion הרסיליאנטי החדש (API)
    run_step("python ingest_api_resilient.py", "Resilient API Ingestion (Pagination & Retries)")
    
    # 3. הרצת מודלי ה-dbt (Staging + Marts)
    run_step("dbt run", "dbt Model Transformations")
    
    # 4. הרצת בדיקות האיכות ב-dbt
    run_step("dbt test", "dbt Data Quality Tests")
    
    total_elapsed = time.time() - total_start
    print(f"\n🎉 FULL PIPELINE COMPLETED SUCCESSFULLY in {total_elapsed:.2f}s!")