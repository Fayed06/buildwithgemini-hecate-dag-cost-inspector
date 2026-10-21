# My agent: DAG Health & BQ Cost Spike Inspector (Hecate Ops)
One-liner: A conversational agent that helps data engineers inspect Airflow DAG migration runs, detect shadow DAG duplication, analyze BigQuery cost spikes, and get verified query optimization recommendations with a catalog of DAG runs and query profiles.

Tool coverage:
- Memory: Tracks active migration streams (e.g. legacy vs new-airflow-k8s-dags), tagged shadow DAGs, baseline daily BQ spend limits, and engineer investigation notes.
- Tools: Look up DAG run execution logs/states, query BQ `INFORMATION_SCHEMA.JOBS_BY_PROJECT` for slot and bytes-billed metrics, and fetch dataset table metadata (partitions/clustering).
- Catalog/UI: Catalog of Airflow DAG execution runs and high-cost BQ queries, rendered as A2UI summary cards and inspection tables.
- Image gen: Generates architecture/migration stage diagrams or visual DAG anomaly charts.
- Sandbox: Computes exact cost differentials ($ per TB billed / slot-ms), partition-pruning efficiency gains, and projected migration savings.

Recommended for every project: memory, storage, tools, image generation, A2UI
Agent-specific / stretch (pick what fits): Code sandbox for BQ cost/slot calculations, BigQuery INFORMATION_SCHEMA lookups, Indonesian/English bilingual explanation.

---

## Frontend UI Enhancement Plan (Roadmap)

### 1. 📊 Status Bar & Real-time Metrics Header
- **Live Service Indicator**: Display real-time status chip `🟢 Agent Engine v1 (Live)` with region badge (`us-east1`) and target system (`Alterra Airflow & BQ DW`).
- **Session KPIs**: Real-time counter of total DAGs inspected and cumulative BQ cost savings identified.

### 2. 📝 Markdown & SQL Syntax Highlighting
- **SQL / Log Formatter**: Highlighting for BigQuery SQL queries (`SELECT`, `WHERE _PARTITIONDATE >= ...`) and Airflow task stack traces.
- **1-Click Copy**: `"📋 Copy SQL"` and `"📋 Copy Recommendation"` buttons on generated query analysis.

### 3. 🎨 Rich A2UI Card Callouts
- **Cost KPI Highlights**:
  - 🚨 *Cost Spike Alert*: High-visibility alert badge (`+$1,240/mo`)
  - 💡 *Partitioning Savings*: Green pill badge (`-82% BQ Scan`)
  - ⏱ *DAG Execution Time*: Amber runtime badge (`4h 12m`)
- **Interactive Action Pills**: Quick drill-downs such as `[ 🔍 View Partition Scheme ]` and `[ ⚡ Re-run Anomaly Check ]`.

### 4. 🗂 Session History & Clear Controls
- **Restart Session**: Header action to start a fresh inspection session without full page refresh.
- **Timestamps**: Subtle timestamp tracking next to user and inspector badges.

### 5. ⚡ Advanced Keyboard & Multi-Line Input
- **Multi-line Query Input**: Support `Shift + Enter` for pasting multi-line SQL queries and DAG logs.
- **Query Auto-expand**: Dynamic input resizing for complex log diagnostics.
