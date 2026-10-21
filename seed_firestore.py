# Copyright 2026 Google LLC
# Seed script for Hecate DAG Health & BQ Cost Spike Inspector

from google.cloud import firestore

PROJECT_ID = "qwiklabs-gcp-04-66e024bcdde3"

SEEDED_DAG_RUNS = [
    {
        "dag_id": "kraken_settlement_recon_daily",
        "dag_name": "Kraken Daily Settlement Recon",
        "repository": "new-airflow-k8s-dags",
        "dataset_target": "dw_kraken",
        "table_target": "fct_settlement_recon",
        "status": "success",
        "execution_date": "2026-10-20",
        "duration_minutes": 18.5,
        "bytes_billed_gb": 420.5,
        "estimated_cost_usd": 2.63,
        "slot_ms_total": 4500000,
        "is_shadow_dag": False,
        "has_anomalous_cost": False,
        "cost_anomaly_reason": None,
        "dedup_strategy": "QUALIFY ROW_NUMBER() OVER (PARTITION BY partner_id, biller_trx_id ORDER BY updated_at DESC) = 1",
        "notes": "Standard daily reconciliation job between kraken_raw and smartbiller_raw."
    },
    {
        "dag_id": "shadow_kraken_settlement_recon_v2",
        "dag_name": "Kraken Settlement Recon (Migration Shadow)",
        "repository": "new-airflow-k8s-dags",
        "dataset_target": "dw_kraken",
        "table_target": "fct_settlement_recon_v2_staging",
        "status": "running_parallel",
        "execution_date": "2026-10-20",
        "duration_minutes": 54.2,
        "bytes_billed_gb": 3850.0,
        "estimated_cost_usd": 24.06,
        "slot_ms_total": 38200000,
        "is_shadow_dag": True,
        "has_anomalous_cost": True,
        "cost_anomaly_reason": "Full scan of krakenv2_raw without partition filter on _PARTITIONDATE; redundant execution parallel to legacy DAG.",
        "dedup_strategy": "SELECT DISTINCT without timestamp cutoff",
        "notes": "Migration shadow DAG running in parallel. High cost spike caused by missing date partition filter."
    },
    {
        "dag_id": "griffin_partner_dim_sync_hourly",
        "dag_name": "Griffin Partner Dimension Sync",
        "repository": "new-airflow-k8s-dags",
        "dataset_target": "dw_kraken",
        "table_target": "dim_partner",
        "status": "warning",
        "execution_date": "2026-10-20",
        "duration_minutes": 8.0,
        "bytes_billed_gb": 85.0,
        "estimated_cost_usd": 0.53,
        "slot_ms_total": 1200000,
        "is_shadow_dag": False,
        "has_anomalous_cost": False,
        "cost_anomaly_reason": None,
        "dedup_strategy": "MERGE into dim_partner ON res_partner_id",
        "notes": "Detects partner ID discrepancies between dw_kraken.dim_partner and griffin_raw.res_partner."
    },
    {
        "dag_id": "smartbiller_raw_ingestion_hourly",
        "dag_name": "Smartbiller Raw Ingestion",
        "repository": "new-airflow-k8s-dags",
        "dataset_target": "smartbiller_raw",
        "table_target": "transactions",
        "status": "success",
        "execution_date": "2026-10-20",
        "duration_minutes": 4.2,
        "bytes_billed_gb": 35.0,
        "estimated_cost_usd": 0.22,
        "slot_ms_total": 450000,
        "is_shadow_dag": False,
        "has_anomalous_cost": False,
        "cost_anomaly_reason": None,
        "dedup_strategy": "Append-only staging table",
        "notes": "Ingests incoming biller callbacks. Lags up to 48 hours for Serpul transactions."
    },
    {
        "dag_id": "gaharu_voucher_transaction_agg_daily",
        "dag_name": "Gaharu Voucher Transaction Daily Agg",
        "repository": "new-airflow-k8s-dags",
        "dataset_target": "gaharu_raw",
        "table_target": "agg_voucher_daily",
        "status": "failed",
        "execution_date": "2026-10-20",
        "duration_minutes": 32.1,
        "bytes_billed_gb": 1620.0,
        "estimated_cost_usd": 10.12,
        "slot_ms_total": 19400000,
        "is_shadow_dag": False,
        "has_anomalous_cost": True,
        "cost_anomaly_reason": "Cross-join explosion on unindexed partner_code during late-settling reconciliation window.",
        "dedup_strategy": "QUALIFY ROW_NUMBER() OVER (PARTITION BY voucher_code ORDER BY created_at DESC) = 1",
        "notes": "Failed on step 4 due to BQ query timeout & memory spill on slot workers."
    }
]

def seed():
    db = firestore.Client(project=PROJECT_ID)
    collection_ref = db.collection("dag_runs")
    print(f"Seeding Firestore collection 'dag_runs' in project '{PROJECT_ID}'...")
    
    for item in SEEDED_DAG_RUNS:
        doc_id = f"{item['dag_id']}_{item['execution_date']}"
        collection_ref.document(doc_id).set(item)
        print(f" -> Seeded document: {doc_id} ({item['dag_name']})")
        
    print("✅ Firestore seeding completed successfully!")

if __name__ == "__main__":
    seed()
