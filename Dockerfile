# 1. בסיס: תמונת פייתון רשמית וקלה (Slim)
FROM python:3.11-slim

# 2. הגדרת תיקיית העבודה בתוך הקונטיינר
WORKDIR /app

# 3. העתקת קובץ הדרישות והתקנת הספריות
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. העתקת כל קבצי הפרויקט לתוך הקונטיינר
COPY . .

# 5. הפקודה שתרץ אוטומטית כשהקונטיינר מתחיל לפעול
CMD ["python", "run_pipeline.py"]