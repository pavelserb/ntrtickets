"""
Biļešu Serviss source — collects daily ticket sales via their internal JSON API.

Requires env vars: BILESU_SERVISS_EMAIL, BILESU_SERVISS_PASSWORD
Per-event params (from config.yaml): event_id, legal_person_id, sale_start
"""

import logging
import os
from datetime import date, timedelta

import requests

log = logging.getLogger("collector.bilesu_serviss")

LOGIN_URL = "https://balticket.bilietai.lt/proxy/sales-terminal/public/login"
VOLUME_URL = "https://apigw.piletilevi.ee/report/sales/volume"
MAX_RETRIES = 3


def collect(params: dict) -> list[dict]:
    """
    Fetch daily sales from the Biļešu Serviss organiser cabinet.

    params must contain: event_id, legal_person_id, sale_start
    """
    email = os.environ["BILESU_SERVISS_EMAIL"]
    password = os.environ["BILESU_SERVISS_PASSWORD"]
    event_id = params["event_id"]
    legal_person_id = params["legal_person_id"]
    sale_start = params["sale_start"]
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    session = requests.Session()
    session.headers.update({"Accept": "application/json", "Content-Type": "application/json"})

    log.info("Logging in as %s …", email)
    token = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.post(LOGIN_URL, json={"email": email, "password": password}, timeout=30)
            resp.raise_for_status()
            token = resp.json()["accessToken"]
            log.info("Login OK (attempt %d)", attempt)
            break
        except (requests.RequestException, KeyError) as exc:
            log.warning("Login attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc)
            if attempt == MAX_RETRIES:
                raise

    log.info("Fetching volume for event %s (%s → %s) …", event_id, sale_start, yesterday)
    api_params = {
        "ownerType": "EVENT_OWNER",
        "period": "DAY",
        "eventIds": event_id,
        "legalPersonId": legal_person_id,
        "saleStart": sale_start,
        "saleEnd": yesterday,
    }
    raw = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(
                VOLUME_URL, params=api_params,
                headers={"Authorization": f"Bearer {token}"}, timeout=30,
            )
            resp.raise_for_status()
            raw = resp.json()
            log.info("Volume data received (attempt %d)", attempt)
            break
        except requests.RequestException as exc:
            log.warning("Fetch attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc)
            if attempt == MAX_RETRIES:
                raise

    money_items: dict = {}
    ticket_items: dict = {}
    for bucket in raw.get("money", []):
        if bucket.get("title") == "current" and bucket.get("items"):
            money_items = bucket["items"]
            break
    for bucket in raw.get("tickets", []):
        if bucket.get("title") == "current" and bucket.get("items"):
            ticket_items = bucket["items"]
            break

    all_dates = sorted(set(money_items.keys()) | set(ticket_items.keys()))
    if not all_dates:
        log.warning("No daily data in response")
        return []

    records = []
    for raw_date in all_dates:
        normalized = raw_date.replace(".", "-")
        revenue_cents = money_items.get(raw_date, 0)
        records.append({
            "date": normalized,
            "tickets": ticket_items.get(raw_date, 0),
            "revenue_eur": round(revenue_cents / 100, 2),
        })

    log.info("Parsed %d records: %s → %s", len(records), records[0]["date"], records[-1]["date"])
    return records
