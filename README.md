# ⚡ Hecate DAG Health & BQ Cost Spike Inspector (Alterra DE Ops)

An autonomous AI agent on Vertex AI Agent Engine that helps Data Engineers inspect Airflow DAG migration runs (`new-airflow-k8s-dags`), detect shadow DAG anomalies, compute BigQuery query cost differentials ($6.25/TB) and partition-pruning savings, check Apache Airflow security advisories, and generate visual pipeline architecture diagrams.

---

## 🚀 Live Endpoints & Deployments

- **🌐 Live Web UI (Cloud Run)**: [https://hecate-dag-cost-inspector-ui-847354227620.us-east1.run.app](https://hecate-dag-cost-inspector-ui-847354227620.us-east1.run.app)
- **⚡ Vertex AI Reasoning Engine**: `projects/847354227620/locations/us-east1/reasoningEngines/2379566363367702528`
- **🧠 Vertex AI Memory Bank**: `agentengine://2379566363367702528`
- **📦 GCS Asset Storage**: `gs://hecate-dag-cost-inspector-assets-24bcdde3`
- **🗄️ Firestore Database**: `(default)` in `nam5` (`projects/qwiklabs-gcp-04-66e024bcdde3`)

---

## 🧪 Testing Guide

This project includes unit, integration, and end-to-end evaluation tests.

### 1. Run All Tests
Execute the entire test suite using `uv`:
```bash
uv run pytest tests/unit tests/integration -v
```

### 2. Run Unit Tests
```bash
uv run pytest tests/unit -v
```

### 3. Run Integration & Stream Tests
Validates real agent runner streaming and message generation:
```bash
uv run pytest tests/integration/test_agent.py -v
```

### 4. Run Server End-to-End Tests
Validates local FastAPI and A2A integration:
```bash
uv run pytest tests/integration/test_server_e2e.py -v
```

### 5. Automated Evaluation & Grading
Evaluate the agent's response quality against defined test datasets:
```bash
agents-cli eval
```

---

## 🛠️ Project Structure

```
hecate-dag-cost-inspector/
├── app/
│   ├── agent.py               # Core ADK agent, tools, code sandbox & memory wiring
│   ├── fast_api_app.py        # Backend runtime server
│   └── app_utils/             # Helpers and A2UI utilities
├── frontend/
│   ├── main.py                # FastAPI proxy for A2A communication
│   └── static/
│       └── index.html         # Custom branded Dark Slate / BQ Amber Chat & A2UI UI
├── tests/
│   ├── unit/                  # Unit test suite
│   ├── integration/           # Integration tests for agent runner and server
│   └── eval/                  # Quality evaluation datasets and configs
├── project_brief.md           # Project architecture & UI enhancement roadmap
├── deployment_metadata.json   # Cloud deployment configuration
└── pyproject.toml             # Python dependencies (uv)
```

---

## 💻 Local Development

### Run Local ADK Playground
```bash
uv run adk web . --port 8080 --reload_agents --memory_service_uri=agentengine://2379566363367702528
```

### Run Local Frontend Proxy
```bash
cd frontend
export AGENT_ENGINE_RESOURCE_NAME="projects/847354227620/locations/us-east1/reasoningEngines/2379566363367702528"
export AGENT_DIRECTORY="app"
export PORT="8080"
uv run python main.py
```
Open [http://localhost:8080](http://localhost:8080) in your browser.

---

## 🚢 Cloud Deployment

### Deploy Backend Agent
```bash
agents-cli deploy
```

### Deploy Frontend to Cloud Run
```bash
cd frontend
gcloud run deploy hecate-dag-cost-inspector-ui \
  --source=. \
  --region=us-east1 \
  --project=qwiklabs-gcp-04-66e024bcdde3 \
  --allow-unauthenticated \
  --set-env-vars="AGENT_ENGINE_RESOURCE_NAME=projects/847354227620/locations/us-east1/reasoningEngines/2379566363367702528,AGENT_DIRECTORY=app"
```
