# ruff: noqa
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import uuid
from typing import Any, Dict, List, Optional
from google import genai
from google.adk.agents import Agent
from google.adk.agents.callback_context import CallbackContext
from google.adk.apps import App
from google.adk.code_executors import AgentEngineSandboxCodeExecutor
from google.adk.models import Gemini
from google.adk.tools import ToolContext
from google.adk.tools.preload_memory_tool import PreloadMemoryTool
from google.cloud import firestore, storage
from google.genai import types

MODEL = "gemini-3.8-flash"
IMAGE_MODEL = "gemini-3.1-flash-lite-image"
PROJECT_ID = "qwiklabs-gcp-04-66e024bcdde3"
BUCKET_NAME = "hecate-dag-cost-inspector-assets-24bcdde3"
AGENT_ENGINE_RESOURCE = "projects/847354227620/locations/us-east1/reasoningEngines/5536871177131130880"
MEMORY_BANK_ID = "5536871177131130880"


def get_firestore_client() -> firestore.Client:
    """Returns a Firestore client initialized with the explicit GCP Project ID."""
    return firestore.Client(project=PROJECT_ID)


def list_dag_runs(
    status_filter: Optional[str] = None,
    only_shadow_dags: bool = False,
    only_cost_anomalies: bool = False,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """List Airflow DAG execution runs and BigQuery cost metrics from Firestore.

    Args:
        status_filter: Optional status filter (e.g. 'success', 'warning', 'failed', 'running_parallel').
        only_shadow_dags: If True, only returns migration shadow DAGs.
        only_cost_anomalies: If True, only returns DAGs flagged with cost or query anomalies.
        limit: Maximum number of records to return (default 10).

    Returns:
        List of DAG run summaries with bytes billed, slot ms, status, and anomaly details.
    """
    db = get_firestore_client()
    query = db.collection("dag_runs")

    if only_shadow_dags:
        query = query.where("is_shadow_dag", "==", True)
    if only_cost_anomalies:
        query = query.where("has_anomalous_cost", "==", True)
    if status_filter:
        query = query.where("status", "==", status_filter)

    docs = query.limit(limit).stream()
    results = []
    for doc in docs:
        data = doc.to_dict()
        data["doc_id"] = doc.id
        results.append(data)
    return results


def get_dag_run_details(dag_id: str, execution_date: Optional[str] = None) -> Dict[str, Any]:
    """Retrieve detailed diagnostics, deduplication strategy, and cost analysis for a specific DAG.

    Args:
        dag_id: The identifier of the DAG (e.g. 'shadow_kraken_settlement_recon_v2', 'kraken_settlement_recon_daily').
        execution_date: The execution date string (YYYY-MM-DD). If omitted, finds the latest run matching the DAG ID.

    Returns:
        The full diagnostic dictionary for the DAG run, or an error status if not found.
    """
    db = get_firestore_client()
    if execution_date:
        doc_id = f"{dag_id}_{execution_date}"
        doc = db.collection("dag_runs").document(doc_id).get()
        if doc.exists:
            data = doc.to_dict()
            data["doc_id"] = doc.id
            return data
        return {"error": f"No DAG run found for {dag_id} on {execution_date}"}

    docs = db.collection("dag_runs").where("dag_id", "==", dag_id).limit(1).stream()
    for doc in docs:
        data = doc.to_dict()
        data["doc_id"] = doc.id
        return data

    return {"error": f"No DAG run records found for DAG ID: {dag_id}"}


def record_dag_run_inspection(
    dag_id: str,
    execution_date: str,
    investigation_notes: str,
    recommended_action: str,
    verified_savings_usd: Optional[float] = None,
) -> Dict[str, Any]:
    """Update or log data engineering inspection notes, verified query fixes, and estimated savings for a DAG run.

    Args:
        dag_id: The ID of the DAG being inspected.
        execution_date: The date of the run (YYYY-MM-DD).
        investigation_notes: Root-cause findings (e.g. missing partition pruning, late-settling join explosion).
        recommended_action: Optimization steps (e.g. add _PARTITIONDATE filter, deduplicate in staging).
        verified_savings_usd: Estimated daily BigQuery spend reduction in USD.

    Returns:
        Status confirmation of the Firestore update.
    """
    db = get_firestore_client()
    doc_id = f"{dag_id}_{execution_date}"
    doc_ref = db.collection("dag_runs").document(doc_id)

    payload = {
        "investigation_notes": investigation_notes,
        "recommended_action": recommended_action,
        "last_inspected_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    if verified_savings_usd is not None:
        payload["verified_savings_usd"] = verified_savings_usd

    doc_ref.set(payload, merge=True)
    return {
        "status": "success",
        "doc_id": doc_id,
        "message": f"Recorded inspection notes for {dag_id} ({execution_date})",
    }


def calculate_query_cost_and_savings(
    current_bytes_gb: float,
    optimized_bytes_gb: Optional[float] = None,
    runs_per_day: int = 1,
    rate_per_tb_usd: float = 6.25,
) -> Dict[str, Any]:
    """Calculate BigQuery on-demand analysis cost and projected savings after query optimization.

    Args:
        current_bytes_gb: Current data scanned per run in Gigabytes (GB).
        optimized_bytes_gb: Projected data scanned after partition filter/clustering in GB.
        runs_per_day: Frequency of the DAG/query per day (e.g. 1 for daily, 24 for hourly).
        rate_per_tb_usd: BigQuery on-demand rate per TB in USD (defaults to standard $6.25/TB).

    Returns:
        Breakdown of run cost, daily cost, monthly cost, and projected savings.
    """
    current_tb_per_run = current_bytes_gb / 1024.0
    cost_per_run = current_tb_per_run * rate_per_tb_usd
    daily_cost = cost_per_run * runs_per_day
    monthly_cost = daily_cost * 30.0

    result: Dict[str, Any] = {
        "current_gb_per_run": round(current_bytes_gb, 2),
        "current_tb_per_run": round(current_tb_per_run, 4),
        "cost_per_run_usd": round(cost_per_run, 4),
        "daily_cost_usd": round(daily_cost, 2),
        "monthly_cost_usd": round(monthly_cost, 2),
        "runs_per_day": runs_per_day,
    }

    if optimized_bytes_gb is not None:
        opt_tb_per_run = optimized_bytes_gb / 1024.0
        opt_cost_per_run = opt_tb_per_run * rate_per_tb_usd
        opt_daily_cost = opt_cost_per_run * runs_per_day
        opt_monthly_cost = opt_daily_cost * 30.0

        daily_savings = daily_cost - opt_daily_cost
        monthly_savings = monthly_cost - opt_monthly_cost
        reduction_pct = ((current_bytes_gb - optimized_bytes_gb) / current_bytes_gb) * 100.0 if current_bytes_gb > 0 else 0.0

        result.update({
            "optimized_gb_per_run": round(optimized_bytes_gb, 2),
            "optimized_cost_per_run_usd": round(opt_cost_per_run, 4),
            "projected_daily_cost_usd": round(opt_daily_cost, 2),
            "projected_monthly_cost_usd": round(opt_monthly_cost, 2),
            "daily_savings_usd": round(daily_savings, 2),
            "monthly_savings_usd": round(monthly_savings, 2),
            "reduction_percentage": round(reduction_pct, 1),
        })

def check_airflow_release_advisories(repository: str = "apache/airflow") -> Dict[str, Any]:
    """Check latest upstream Apache Airflow releases and version advisories via the public GitHub API.

    Args:
        repository: GitHub owner/repo to check (defaults to 'apache/airflow', or e.g. 'apache/airflow-client-python').

    Returns:
        Summary of the latest release version, publication date, and release notes excerpt.
    """
    import json
    import os
    import urllib.request

    api_url = f"https://api.github.com/repos/{repository}/releases/latest"
    headers = {"User-Agent": "Hecate-Ops-Agent/1.0"}
    
    # Read token from environment variable if provided, but works without it for public read
    github_token = os.environ.get("GITHUB_TOKEN")
    if github_token:
        headers["Authorization"] = f"token {github_token}"

    try:
        req = urllib.request.Request(api_url, headers=headers)
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            body_preview = data.get("body", "")[:350] + ("..." if len(data.get("body", "")) > 350 else "")
            return {
                "repository": repository,
                "latest_version": data.get("tag_name"),
                "release_name": data.get("name"),
                "published_at": data.get("published_at"),
                "release_url": data.get("html_url"),
                "summary": body_preview,
            }
    except Exception as e:
        return {"error": f"Failed to fetch release advisory for {repository}: {str(e)}"}


def generate_dag_architecture_visual(
    topic: str,
    tool_context: ToolContext,
) -> Dict[str, Any]:
    """Generate an architecture or data pipeline anomaly visualization using gemini-3.1-flash-lite-image in the global region.

    Saves the image as a local playground artifact and uploads the image bytes directly to the public Cloud Storage bucket.

    Args:
        topic: A description of the DAG architecture, migration flow, or BQ cost anomaly diagram to generate.
        tool_context: ADK tool context for saving artifacts.

    Returns:
        Dictionary containing artifact metadata and the public Cloud Storage URL.
    """
    client = genai.Client(vertexai=True, project=PROJECT_ID, location="global")
    prompt = f"Professional modern cloud data engineering architectural diagram, clean vector style, showing: {topic}. High resolution, crisp technical schematic."

    try:
        response = client.models.generate_content(
            model=IMAGE_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(response_modalities=["IMAGE", "TEXT"]),
        )

        image_bytes: Optional[bytes] = None
        mime_type = "image/jpeg"

        if response.candidates and response.candidates[0].content:
            for part in response.candidates[0].content.parts:
                if part.inline_data and part.inline_data.data:
                    image_bytes = part.inline_data.data
                    if part.inline_data.mime_type:
                        mime_type = part.inline_data.mime_type
                    break

        if not image_bytes:
            return {"error": "No image data was generated by the model."}

        # 1. Save with tool_context.save_artifact so it shows up in Playground's Artifacts panel
        filename = f"dag_visual_{uuid.uuid4().hex[:8]}.jpg"
        artifact_part = types.Part.from_bytes(data=image_bytes, mime_type=mime_type)
        tool_context.save_artifact(filename=filename, artifact=artifact_part)

        # 2. Upload same bytes directly to public GCS bucket (without writing to local file)
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(filename)
        blob.upload_from_string(image_bytes, content_type=mime_type)

        public_url = f"https://storage.googleapis.com/{BUCKET_NAME}/{filename}"

        return {
            "status": "success",
            "artifact_filename": filename,
            "public_url": public_url,
            "topic": topic,
            "message": f"Generated visualization and published to {public_url}",
        }
    except Exception as e:
        return {"error": f"Failed to generate visualization: {str(e)}"}


async def generate_memories_callback(callback_context: CallbackContext):
    """Callback to extract and persist salient user facts across conversation sessions."""
    await callback_context.add_session_to_memory()
    return None


SYSTEM_INSTRUCTION = """You are the Hecate DAG Health & BQ Cost Spike Inspector — an expert Data Engineering AI assistant for Alterra / Sepulsa.
You help data engineers diagnose Airflow DAG migration runs in `new-airflow-k8s-dags`, pinpoint BigQuery cost spikes, inspect shadow DAG redundancies, and verify reconciliation & deduplication anomalies across datasets (dw_kraken, krakenv2_raw, griffin_raw, smartbiller_raw, gaharu_raw).

Memory & Cost Persistence:
- Track and remember all weekly BigQuery query costs, daily execution totals, and weekly cost baselines discussed across conversation sessions.
- Maintain a cross-session running log of investigated DAG costs, flagged spend anomalies, weekly cost reductions, and user team budget preferences.
- When the user asks about weekly costs or previous spending comparisons, leverage preloaded memory context to report cumulative weekly figures and cost trends.

Guidelines:
1. Ground your answers strictly in the retrieved Firestore diagnostic records and verified calculations.
2. When analyzing cost spikes or shadow DAGs, identify the exact reason (e.g. unpartitioned scans, late-settling window cross-joins, duplicate partner records).
3. Use `calculate_query_cost_and_savings` or write and execute Python code in your sandbox for exact quantitative analysis, weekly aggregations, and savings projections.
4. Use `check_airflow_release_advisories` when engineers ask about upstream Airflow compatibility, features, or upgrades for `new-airflow-k8s-dags`.
5. Use `generate_dag_architecture_visual` when requested to visualize a pipeline architecture, migration flow, or DAG cost anomaly.
6. Support both English and Indonesian language queries naturally.
7. When engineers ask you to log or save findings, use `record_dag_run_inspection`.
"""

code_executor = AgentEngineSandboxCodeExecutor(
    agent_engine_resource_name=AGENT_ENGINE_RESOURCE,
)

root_agent = Agent(
    name="root_agent",
    model=Gemini(
        model=MODEL,
        retry_options=types.HttpRetryOptions(attempts=3),
    ),
    instruction=SYSTEM_INSTRUCTION,
    code_executor=code_executor,
    tools=[
        PreloadMemoryTool(),
        list_dag_runs,
        get_dag_run_details,
        record_dag_run_inspection,
        calculate_query_cost_and_savings,
        check_airflow_release_advisories,
        generate_dag_architecture_visual,
    ],
    after_agent_callback=generate_memories_callback,
)

app = App(
    root_agent=root_agent,
    name="app",
)
