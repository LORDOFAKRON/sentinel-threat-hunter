import base64
import json
import functions_framework
from google.cloud import bigquery

# BigQuery İstemcisi
client = bigquery.Client()

# --- AYARLAR ---
# Buraya kendi PROJE ID'ni yazmalısın!
# Format: "proje-id.dataset-id.tablo-id"
TABLE_ID = "project-24b3140b-f1a6-4e34-8ec.sentinel_dataset.logs"
# ----------------

@functions_framework.cloud_event
def subscribe(cloud_event):
    # 1. Pub/Sub'dan gelen şifreli mesajı çöz
    pubsub_message = base64.b64decode(cloud_event.data["message"]["data"]).decode()
    
    # 2. JSON formatına çevir
    log_data = json.loads(pubsub_message)
    
    # 3. BigQuery'e yazılacak satırı hazırla
    rows_to_insert = [log_data]

    # 4. Veriyi tabloya ekle
    errors = client.insert_rows_json(TABLE_ID, rows_to_insert)
    
    if errors == []:
        print(f"✅ Log BigQuery'e eklendi: {log_data.get('ip_address')}")
    else:
        print(f"❌ Hata oluştu: {errors}")

