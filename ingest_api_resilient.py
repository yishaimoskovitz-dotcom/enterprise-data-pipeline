import time
import requests
import duckdb
import pandas as pd

# API דמה אמיתי מבית JSONPlaceholder שתומך ב-Pagination (?_page=1&_limit=10)
BASE_URL = "https://jsonplaceholder.typicode.com/posts"
DB_FILE = "dev.duckdb"
MAX_RETRIES = 3
PAGE_LIMIT = 15  # 15 פריטים בעמוד כדי לראות מעבר עמודים


def fetch_page_with_retry(page: int, limit: int = PAGE_LIMIT) -> list:
    """מושך עמוד בודד מה-API עם מנגנון Exponential Backoff & Retry."""
    url = f"{BASE_URL}?_page={page}&_limit={limit}"
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"Fetching Page {page} (Attempt {attempt}/{MAX_RETRIES})...")
            
            # verify=False במידה ויש בעיות תעודה מקומיות
            response = requests.get(url, timeout=10, verify=False)
            
            # 1. טיפול ב-Rate Limiting (HTTP 429)
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                print(f"⚠️ Rate limit hit! Sleeping for {retry_after} seconds...")
                time.sleep(retry_after)
                continue
                
            # 2. בדיקה אם יש שגיאת HTTP בלתי צפויה (למשל 404, 500)
            response.raise_for_status()
            
            # אם הגענו לכאן - הקריאה הצליחה!
            return response.json()

        except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
            print(f"❌ Error on page {page}, attempt {attempt}: {e}")
            if attempt < MAX_RETRIES:
                sleep_time = 2 ** attempt  # Exponential Backoff: 2s, 4s, 8s...
                print(f"Waiting {sleep_time} seconds before retrying...")
                time.sleep(sleep_time)
            else:
                print(f"🚨 Failed to fetch page {page} after {MAX_RETRIES} attempts.")
                raise e


def fetch_all_pages() -> list:
    """מבצע בלולאה Pagination עד שכל הנתונים מגיעים."""
    all_records = []
    page = 1
    
    while True:
        data = fetch_page_with_retry(page=page)
        
        # תנאי עצירה: ה-API החזיר רשימה ריקה -> הגענו לסוף הנתונים
        if not data:
            print(f"✅ Reached end of data at page {page}.")
            break
            
        all_records.extend(data)
        print(f"Fetched {len(data)} items from page {page}. Total so far: {len(all_records)}")
        page += 1
        
    return all_records


def load_to_duckdb(records: list):
    """טוען את הנתונים שנאספו לטבלת raw ב-DuckDB תחת סכמת main."""
    if not records:
        print("No records to load.")
        return
        
    df = pd.DataFrame(records)
    
    con = duckdb.connect(DB_FILE)
    
    # טעינה ישירה לסכמת main
    con.execute("CREATE OR REPLACE TABLE main.raw_api_posts AS SELECT * FROM df")
    
    count = con.execute("SELECT count(*) FROM main.raw_api_posts").fetchone()[0]
    con.close()
    
    print(f"🚀 Successfully loaded {count} records into 'main.raw_api_posts' in DuckDB!")

if __name__ == "__main__":
    print("--- Starting Resilient Ingestion Pipeline ---")
    all_data = fetch_all_pages()
    load_to_duckdb(all_data)