"""
Passo source — collects daily ticket sales from the Passo Power BI Report Server.

Authentication is NTLM (Windows Integrated Auth).
Data is extracted by replaying the SemanticQuery that the Power BI daily-sales
visual sends to the /querydata endpoint.

Requires env vars: PASSO_USERNAME, PASSO_PASSWORD
Per-event params (from config.yaml):
    report_id       — GUID of the Power BI report
    model_id        — numeric model ID (visible in the original querydata payload)
    event_name      — (optional) DimEvent.EventStartDate value to filter by
                      e.g. "Andrea Bocelli | Romanza - 30th Anniversary World Tour 2026-05-30 20:00"
    sale_start      — (optional) earliest booking date, default "2024-01-01"
"""

import logging
import os
import uuid
from datetime import datetime, timezone

import requests
from requests_ntlm import HttpNtlmAuth

log = logging.getLogger("collector.passo")

BASE_URL = "https://provapowerbi.passo.com.tr"
MAX_RETRIES = 3


def _build_payload(params: dict) -> dict:
    model_id = str(params["model_id"])
    sale_start = params.get("sale_start", "2024-01-01")
    event_name = params.get("event_name")

    where = [
        {
            "Condition": {
                "Comparison": {
                    "ComparisonKind": 2,
                    "Left": {"Column": {"Expression": {"SourceRef": {"Source": "c"}}, "Property": "CreatedonBookingDatetime"}},
                    "Right": {"Literal": {"Value": f"datetime'{sale_start}T00:00:00'"}},
                }
            }
        },
        {
            "Condition": {
                "In": {
                    "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "d1"}}, "Property": "Name"}}],
                    "Values": [[{"Literal": {"Value": "'Satışa Açık Etkinlik'"}}]],
                }
            }
        },
        {
            "Condition": {
                "Comparison": {
                    "ComparisonKind": 2,
                    "Left": {"Column": {"Expression": {"SourceRef": {"Source": "d"}}, "Property": "StartDate"}},
                    "Right": {"Literal": {"Value": "datetime'2022-01-01T00:00:00'"}},
                }
            }
        },
        {
            "Condition": {
                "In": {
                    "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "d21"}}, "Property": "Name"}}],
                    "Values": [[{"Literal": {"Value": "'Event'"}}]],
                }
            }
        },
    ]

    if event_name:
        where.insert(0, {
            "Condition": {
                "In": {
                    "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "d"}}, "Property": "EventStartDate"}}],
                    "Values": [[{"Literal": {"Value": f"'{event_name}'"}}]],
                }
            }
        })

    query = {
        "Version": 2,
        "From": [
            {"Name": "d", "Entity": "DimEvent", "Type": 0},
            {"Name": "c", "Entity": "CubeFactTFF", "Type": 0},
            {"Name": "r", "Entity": "RawVenue", "Type": 0},
            {"Name": "d2", "Entity": "DimVariant", "Type": 0},
            {"Name": "d3", "Entity": "DimFreeOfCharge", "Type": 0},
            {"Name": "d1", "Entity": "DimSoldPeriod", "Type": 0},
            {"Name": "d21", "Entity": "DimProductType", "Type": 0},
        ],
        "Select": [
            {"Aggregation": {"Expression": {"Column": {"Expression": {"SourceRef": {"Source": "c"}}, "Property": "TicketCount"}}, "Function": 0}, "Name": "Sum(CubeFactTFF.TicketCount)"},
            {"Column": {"Expression": {"SourceRef": {"Source": "d"}}, "Property": "EventCodeName"}, "Name": "DimEvent.EventCodeName"},
            {"Column": {"Expression": {"SourceRef": {"Source": "r"}}, "Property": "Name"}, "Name": "RawVenue.Name"},
            {"Column": {"Expression": {"SourceRef": {"Source": "d"}}, "Property": "StartDate"}, "Name": "DimEvent.StartDate"},
            {"Column": {"Expression": {"SourceRef": {"Source": "d2"}}, "Property": "Name"}, "Name": "DimVariant.Name"},
            {"Column": {"Expression": {"SourceRef": {"Source": "d3"}}, "Property": "Name"}, "Name": "DimFreeOfCharge.Name"},
            {"Aggregation": {"Expression": {"Column": {"Expression": {"SourceRef": {"Source": "c"}}, "Property": "BasePrice"}}, "Function": 0}, "Name": "Sum(CubeFactTFF.BasePrice)"},
            {"Column": {"Expression": {"SourceRef": {"Source": "c"}}, "Property": "CreatedonBookingDate"}, "Name": "CubeFactTFF.CreatedonBookingDate", "NativeReferenceName": "Satış Tarihi1"},
        ],
        "Where": where,
        "OrderBy": [{"Direction": 2, "Expression": {"Column": {"Expression": {"SourceRef": {"Source": "c"}}, "Property": "CreatedonBookingDate"}}}],
    }

    return {
        "version": "1.0.0",
        "queries": [
            {
                "Query": {
                    "Commands": [
                        {
                            "SemanticQueryDataShapeCommand": {
                                "Query": query,
                                "Binding": {
                                    "Primary": {"Groupings": [{"Projections": [7], "Subtotal": 1}]},
                                    "Secondary": {"Groupings": [{"Projections": [0, 5, 6]}]},
                                    "DataReduction": {
                                        "DataVolume": 3,
                                        "Primary": {"Window": {"Count": 500}},
                                        "Secondary": {"Top": {"Count": 100}},
                                    },
                                    "Version": 1,
                                },
                                "ExecutionMetricsKind": 1,
                            }
                        }
                    ],
                    "QueryId": "",
                    "ApplicationContext": {"Sources": [{"VisualId": "263165d520a80eb8b1e5"}]},
                },
            }
        ],
        "cancelQueries": [],
        "modelId": model_id,
        "userPreferredLocale": "en-US",
    }


def _parse_daily_sales(data: dict) -> list[dict]:
    """
    Parse the Power BI DSR v2 response into a flat list of daily records.

    The response structure (abbreviated):
        results[0].result.data.dsr.DS[0].PH[
            {DM0: [subtotal]},        ← skip
            {DM1: [{G0: ts, X: [{C: [tickets, revenue]}]}, ...]}  ← daily rows
        ]
    """
    results = data.get("results", [])
    if not results:
        return []

    ds_list = (
        results[0]
        .get("result", {})
        .get("data", {})
        .get("dsr", {})
        .get("DS", [])
    )
    if not ds_list:
        return []

    records: list[dict] = []
    for ph_entry in ds_list[0].get("PH", []):
        dm1_rows = ph_entry.get("DM1")
        if dm1_rows is None:
            continue

        for row in dm1_rows:
            timestamp_ms = row.get("G0")
            if timestamp_ms is None:
                continue

            x_list = row.get("X", [])
            if not x_list:
                continue

            c = x_list[0].get("C", [])
            if len(c) < 2:
                continue

            dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
            records.append({
                "date": dt.strftime("%Y-%m-%d"),
                "tickets": int(c[0]),
                "revenue_eur": float(c[1]),
            })

    records.sort(key=lambda r: r["date"])
    return records


def collect(params: dict) -> list[dict]:
    """
    Fetch daily ticket sales from the Passo Power BI Report Server.

    params must contain: report_id, model_id
    params may contain:  event_name, sale_start
    """
    username = os.environ["PASSO_USERNAME"]
    password = os.environ["PASSO_PASSWORD"]
    report_id = params["report_id"]

    url = f"{BASE_URL}/powerbi/api/explore/reports/{report_id}/querydata"
    payload = _build_payload(params)

    session = requests.Session()
    session.auth = HttpNtlmAuth(username, password)
    session.headers.update({
        "Content-Type": "application/json;charset=UTF-8",
        "Accept": "application/json, text/plain, */*",
        "ActivityId": str(uuid.uuid4()),
        "RequestId": str(uuid.uuid4()),
        "X-PowerBI-ResourceKey": "any",
    })

    log.info("Querying Power BI report %s …", report_id)
    raw = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.post(url, json=payload, timeout=60)
            resp.raise_for_status()
            raw = resp.json()
            log.info("Data received (attempt %d)", attempt)
            break
        except requests.RequestException as exc:
            log.warning("Attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc)
            if attempt == MAX_RETRIES:
                raise

    records = _parse_daily_sales(raw)
    if records:
        log.info("Parsed %d daily records: %s → %s", len(records), records[0]["date"], records[-1]["date"])
    else:
        log.warning("No daily records found in Power BI response")

    return records
