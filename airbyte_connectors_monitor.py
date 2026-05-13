import argparse
import datetime
import json
import os
import time
from pathlib import Path

import requests
import yaml

from pipeline_monitor import PipelineMonitor


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = BASE_DIR / "airbyte_clients.yaml"


def get_schedule_client_name(schedule_client):
    if isinstance(schedule_client, str):
        return schedule_client

    return schedule_client.get("client_name")


def filter_client_connectors_for_schedule(client, schedule_client_config):
    if isinstance(schedule_client_config, str):
        return client

    include_connectors = {
        name.strip().lower()
        for name in schedule_client_config.get("include_connectors", [])
    }

    exclude_connectors = {
        name.strip().lower()
        for name in schedule_client_config.get("exclude_connectors", [])
    }

    connectors = client.get("connectors", [])

    if include_connectors:
        connectors = [
            connector for connector in connectors
            if connector["name"].strip().lower() in include_connectors
        ]

    if exclude_connectors:
        connectors = [
            connector for connector in connectors
            if connector["name"].strip().lower() not in exclude_connectors
        ]

    if not connectors:
        raise ValueError(
            f"No connectors selected for client '{client['client_name']}' "
            f"in schedule group."
        )

    filtered_client = dict(client)
    filtered_client["connectors"] = connectors

    return filtered_client


def run_clients_by_schedule_group(schedule_group, config_path=None):
    config = load_config(config_path or DEFAULT_CONFIG_PATH)

    group_config = config.get("schedule_groups", {}).get(schedule_group)
    if not group_config:
        raise ValueError(f"No schedule group found: {schedule_group}")

    schedule_clients = group_config.get("clients", [])

    schedule_client_map = {
        get_schedule_client_name(schedule_client).strip().lower(): schedule_client
        for schedule_client in schedule_clients
        if get_schedule_client_name(schedule_client)
    }

    clients = [
        filter_client_connectors_for_schedule(
            client=client,
            schedule_client_config=schedule_client_map[
                client.get("client_name", "").strip().lower()
            ],
        )
        for client in config["clients"]
        if client.get("client_name", "").strip().lower() in schedule_client_map
    ]

    print("DEBUG schedule_group:", schedule_group)
    print("DEBUG selected_client_names:", sorted(schedule_client_map.keys()))
    print("DEBUG matched_clients:", [client["client_name"] for client in clients])

    if not clients:
        raise ValueError(f"No clients found for schedule group: {schedule_group}")

    results = []

    for client in clients:
        print("DEBUG running client:", client["client_name"])
        print("DEBUG client connectors:", [c["name"] for c in client.get("connectors", [])])
        results.append(run(client))

    return results


def to_utc(ts):
    if not ts:
        return None

    try:
        return datetime.datetime.fromtimestamp(
            int(ts) / 1000,
            tz=datetime.timezone.utc,
        ).isoformat()
    except Exception:
        pass

    try:
        return datetime.datetime.fromtimestamp(
            int(ts),
            tz=datetime.timezone.utc,
        ).isoformat()
    except Exception:
        pass

    try:
        return datetime.datetime.fromisoformat(
            str(ts).replace("Z", "+00:00")
        ).isoformat()
    except Exception:
        return str(ts)


def parse_time(ts):
    if not ts:
        return None

    try:
        return datetime.datetime.fromtimestamp(
            int(ts) / 1000,
            tz=datetime.timezone.utc,
        )
    except Exception:
        pass

    try:
        return datetime.datetime.fromtimestamp(
            int(ts),
            tz=datetime.timezone.utc,
        )
    except Exception:
        pass

    try:
        return datetime.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return None


def duration_minutes_between(start_time, end_time):
    start_dt = parse_time(start_time)
    end_dt = parse_time(end_time)

    if not start_dt or not end_dt:
        return None

    return round((end_dt - start_dt).total_seconds() / 60, 2)


def extract_duration_minutes(job):
    if not job:
        return None

    raw_duration = (
        job.get("duration")
        or job.get("durationSeconds")
        or job.get("durationInSeconds")
    )

    if raw_duration is not None:
        try:
            return round(float(raw_duration) / 60, 2)
        except Exception:
            pass

    raw_duration_ms = job.get("durationMs") or job.get("durationInMs")
    if raw_duration_ms is not None:
        try:
            return round(float(raw_duration_ms) / 60000, 2)
        except Exception:
            pass

    start_time = job.get("createdAt") or job.get("startedAt") or job.get("startTime")
    end_time = (
        job.get("lastUpdatedAt")
        or job.get("updatedAt")
        or job.get("endedAt")
        or job.get("endTime")
    )

    return duration_minutes_between(start_time, end_time)


AIRBYTE_STATUS_MAP = {
    "succeeded": "PASS",
    "failed": "FAIL",
    "cancelled": "FAIL",
    "incomplete": "FAIL",
    "running": "RUNNING",
    "pending": "RUNNING",
}

FAILURE_STATUSES = {
    "failed",
    "cancelled",
    "incomplete",
    "STATUS_CHECK_FAILED",
    "NO_RUN_FOUND",
    "ERROR",
}


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc)


def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_env_value(env_name, required=True):
    value = os.getenv(env_name)

    if required and not value:
        raise ValueError(f"Missing required environment variable: {env_name}")

    return value


def request_with_retries(
    method,
    url,
    headers,
    params=None,
    json_body=None,
    data=None,
    retries=3,
):
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_body,
                data=data,
                timeout=60,
            )

            response.raise_for_status()
            return response

        except Exception as e:
            last_error = e

            if attempt == retries:
                break

            time.sleep(2 ** attempt)

    raise last_error


def get_airbyte_access_token():
    token_url = get_env_value("AIRBYTE_TOKEN_URL")

    payload = {
        "grant_type": "password",
        "client_id": get_env_value("AIRBYTE_CLIENT_ID"),
        "client_secret": get_env_value("AIRBYTE_CLIENT_SECRET"),
        "username": get_env_value("AIRBYTE_USERNAME"),
        "password": get_env_value("AIRBYTE_PASSWORD"),
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }

    response = request_with_retries(
        method="POST",
        url=token_url,
        headers=headers,
        data=payload,
    )

    token_data = response.json()

    if "access_token" not in token_data:
        raise ValueError(f"Airbyte token response did not contain access_token: {token_data}")

    return token_data["access_token"]


def get_airbyte_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }


def list_airbyte_jobs(api_url, token, connection_id):
    response = request_with_retries(
        method="GET",
        url=f"{api_url.rstrip('/')}/api/public/v1/jobs",
        headers=get_airbyte_headers(token),
        params={
            "connectionId": connection_id,
            "jobType": "sync",
            "orderBy": "createdAt|DESC",
            "limit": 1,
            "offset": 0,
        },
    )

    return response.json().get("data", [])


def get_latest_job(jobs):
    return jobs[0] if jobs else None


def extract_total_stats(job):
    if not job:
        return {"records_synced": 0}

    records = (
        job.get("rowsSynced")
        or job.get("recordsSynced")
        or job.get("recordsCommitted")
        or 0
    )

    return {"records_synced": int(records)}


def extract_failure_summary(job):
    if not job:
        return None

    failure_summary = job.get("failureSummary")
    if not failure_summary:
        return None

    failures = failure_summary.get("failures") or []

    return {
        "partial_success": failure_summary.get("partialSuccess"),
        "failures": [
            {
                "failure_origin": failure.get("failureOrigin"),
                "failure_type": failure.get("failureType"),
                "external_message": failure.get("externalMessage"),
                "internal_message": failure.get("internalMessage"),
                "timestamp": failure.get("timestamp"),
            }
            for failure in failures
        ],
    }


def normalize_connector_status(connector, jobs):
    latest_job = get_latest_job(jobs)

    if not latest_job:
        return {
            "connector_name": connector["name"],
            "connection_id": connector["connection_id"],
            "status": "NO_RUN_FOUND",
            "last_run_result": "UNKNOWN",
            "last_run_time": None,
            "job_id": None,
            "records_synced": 0,
            "duration_minutes": None,
            "failure_summary": None,
        }

    status = latest_job.get("status")
    result = AIRBYTE_STATUS_MAP.get(status, "UNKNOWN")

    totals = extract_total_stats(latest_job)
    failure_summary = extract_failure_summary(latest_job)

    start_time = latest_job.get("createdAt")
    end_time = (
        latest_job.get("lastUpdatedAt")
        or latest_job.get("updatedAt")
        or latest_job.get("endedAt")
    )

    last_run_time = end_time or start_time

    return {
        "connector_name": connector["name"],
        "connection_id": connector["connection_id"],
        "job_id": latest_job.get("jobId") or latest_job.get("id"),
        "status": status,
        "last_run_result": result,
        "last_run_time": to_utc(last_run_time),
        "records_synced": totals["records_synced"],
        "duration_minutes": extract_duration_minutes(latest_job),
        "failure_summary": failure_summary,
    }


def collect_one_connector(connector, token=None, api_url=None):
    token = token or get_airbyte_access_token()
    api_url = api_url or get_env_value("AIRBYTE_API_URL")

    try:
        jobs = list_airbyte_jobs(
            api_url=api_url,
            token=token,
            connection_id=connector["connection_id"],
        )

        return normalize_connector_status(connector, jobs)

    except Exception as e:
        return {
            "connector_name": connector["name"],
            "connection_id": connector["connection_id"],
            "status": "ERROR",
            "last_run_result": "FAIL",
            "last_run_time": None,
            "job_id": None,
            "records_synced": 0,
            "duration_minutes": None,
            "failure_summary": str(e),
        }


def collect(client):
    token = get_airbyte_access_token()
    api_url = get_env_value("AIRBYTE_API_URL")

    results = []

    for connector in client["connectors"]:
        results.append(
            collect_one_connector(
                connector=connector,
                token=token,
                api_url=api_url,
            )
        )

    return results


def build_payload(connectors):
    failed = [
        connector for connector in connectors
        if connector["last_run_result"] != "PASS"
    ]

    return {
        "health": "FAILED" if failed else "HEALTHY",
        "connectors_total": len(connectors),
        "connectors_failed": len(failed),
        "connectors": connectors,
    }


def run(client, selected_connector_name=None, selected_connection_id=None):
    api_key = get_env_value(client["pipeline_api_key_env"])

    if selected_connector_name or selected_connection_id:
        matched_connectors = []

        for connector in client["connectors"]:
            name_match = (
                selected_connector_name
                and connector["name"].lower() == selected_connector_name.lower()
            )
            id_match = (
                selected_connection_id
                and connector["connection_id"] == selected_connection_id
            )

            if name_match or id_match:
                matched_connectors.append(connector)

        if not matched_connectors:
            raise ValueError(f"No connector matched for client '{client['client_name']}'.")

        connector = matched_connectors[0]
        connector_pipeline_id = connector.get("pipeline_id")

        if not connector_pipeline_id:
            raise ValueError(f"Connector '{connector['name']}' does not have pipeline_id.")

        monitor = PipelineMonitor(
            pipeline_id=connector_pipeline_id,
            job_name=f"Airbyte Connector Status | {client['client_name']} | {connector['name']}",
            api_key=api_key,
            job_id=utc_now().strftime("%Y%m%d_%H%M%S"),
            client_name=client["client_name"],
        )

        with monitor.monitor_execution():
            connector_result = collect_one_connector(connector)
            payload = build_payload([connector_result])
            monitor.update_metadata(payload)
            return payload

    client_pipeline_id = client.get("pipeline_id")

    if client_pipeline_id:
        monitor = PipelineMonitor(
            pipeline_id=client_pipeline_id,
            job_name=f"Airbyte Connector Status | {client['client_name']}",
            api_key=api_key,
            job_id=utc_now().strftime("%Y%m%d_%H%M%S"),
            client_name=client["client_name"],
        )

        with monitor.monitor_execution():
            connectors = collect(client)
            payload = build_payload(connectors)
            monitor.update_metadata(payload)
            return payload

    results = []

    for connector in client["connectors"]:
        connector_pipeline_id = connector.get("pipeline_id")

        if not connector_pipeline_id:
            raise ValueError(
                f"Missing pipeline_id for connector '{connector['name']}' "
                f"and no client-level pipeline_id was provided."
            )

        monitor = PipelineMonitor(
            pipeline_id=connector_pipeline_id,
            job_name=f"Airbyte Connector Status | {client['client_name']} | {connector['name']}",
            api_key=api_key,
            job_id=utc_now().strftime("%Y%m%d_%H%M%S"),
            client_name=client["client_name"],
        )

        with monitor.monitor_execution():
            connector_result = collect_one_connector(connector)
            payload = build_payload([connector_result])
            monitor.update_metadata(payload)
            results.append(payload)

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--client-name")
    parser.add_argument("--connector-name")
    parser.add_argument("--connection-id")

    args = parser.parse_args()

    config = load_config(args.config)
    clients = config["clients"]

    if args.client_name:
        clients = [
            client for client in clients
            if client["client_name"].lower() == args.client_name.lower()
        ]

    results = [
        run(
            client,
            selected_connector_name=args.connector_name,
            selected_connection_id=args.connection_id,
        )
        for client in clients
    ]

    print(json.dumps(results, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()