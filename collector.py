"""
Ticket sales collector — orchestrator.

Reads config.yaml, iterates over events and their sources,
collects daily sales data, and stores everything in a local SQLite database.
"""

import argparse
import importlib
import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml
from dotenv import load_dotenv

import html_report
import telegram_report

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "sales.db"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
CONFIG_PATH = BASE_DIR / "config.yaml"

log = logging.getLogger("collector")

SOURCE_MODULES = {
    "bilesu_serviss": "sources.bilesu_serviss",
    "mticket": "sources.mticket",
}


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s")

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(fmt)

    file_handler = logging.FileHandler(
        LOG_DIR / f"collector_{datetime.now().strftime('%Y%m%d')}.log",
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)

    log.setLevel(logging.DEBUG)
    log.addHandler(console)
    log.addHandler(file_handler)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def _load_config(event_slug: str | None = None) -> list[dict]:
    if not CONFIG_PATH.exists():
        log.error("Config file not found: %s", CONFIG_PATH)
        sys.exit(1)

    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    events = cfg.get("events", [])
    if not events:
        log.error("No events defined in config.yaml")
        sys.exit(1)

    if event_slug:
        events = [e for e in events if e["slug"] == event_slug]
        if not events:
            log.error("Event '%s' not found in config.yaml", event_slug)
            sys.exit(1)

    return events


# ---------------------------------------------------------------------------
# SQLite storage
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS daily_sales (
    date           TEXT    NOT NULL,
    event_slug     TEXT    NOT NULL,
    source         TEXT    NOT NULL,
    tickets        INTEGER NOT NULL,
    revenue_cents  INTEGER NOT NULL,
    collected_at   TEXT    NOT NULL,
    PRIMARY KEY (date, event_slug, source)
);
"""


def _init_db(db: sqlite3.Connection) -> None:
    db.executescript(_DDL)


def _upsert_records(
    db: sqlite3.Connection,
    records: list[dict],
    event_slug: str,
    source: str,
    collected_at: str,
) -> tuple[int, int]:
    inserted = updated = 0
    for rec in records:
        revenue_cents = round(rec["revenue_eur"] * 100)
        cur = db.execute(
            "SELECT tickets, revenue_cents FROM daily_sales "
            "WHERE date = ? AND event_slug = ? AND source = ?",
            (rec["date"], event_slug, source),
        )
        existing = cur.fetchone()

        if existing is None:
            db.execute(
                "INSERT INTO daily_sales (date, event_slug, source, tickets, revenue_cents, collected_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (rec["date"], event_slug, source, rec["tickets"], revenue_cents, collected_at),
            )
            inserted += 1
        elif existing != (rec["tickets"], revenue_cents):
            db.execute(
                "UPDATE daily_sales SET tickets = ?, revenue_cents = ?, collected_at = ? "
                "WHERE date = ? AND event_slug = ? AND source = ?",
                (rec["tickets"], revenue_cents, collected_at, rec["date"], event_slug, source),
            )
            updated += 1

    db.commit()
    return inserted, updated


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _print_report(
    event: dict,
    results: list[tuple[str, str, list[dict]]],
) -> None:
    """
    results: list of (src_type, provider_name, records) tuples.
    """
    name = event["name"]
    target = event.get("sales_target") or {}
    target_tickets = target.get("tickets")
    target_revenue = target.get("revenue")

    print(f"\n{'=' * 60}")
    print(f"  {name}")
    print(f"{'=' * 60}")

    grand_tickets = 0
    grand_revenue = 0.0
    sources_with_data = 0

    for _src_type, provider_name, records in results:
        if not records:
            print(f"\n  [{provider_name}] No data.\n")
            continue

        sources_with_data += 1
        total_tickets = sum(r["tickets"] for r in records)
        total_revenue = sum(r["revenue_eur"] for r in records)
        grand_tickets += total_tickets
        grand_revenue += total_revenue

        print(f"\n  [{provider_name}]")
        print(f"  {'Date':<14} {'Tickets':>8} {'Revenue (EUR)':>14}")
        print(f"  {'-' * 40}")
        for r in records:
            print(f"  {r['date']:<14} {r['tickets']:>8} {r['revenue_eur']:>14.2f}")
        print(f"  {'-' * 40}")
        print(f"  {'TOTAL':<14} {total_tickets:>8} {total_revenue:>14.2f}")

    if sources_with_data > 1:
        print(f"\n  {'─' * 40}")
        print(f"  {'ALL SOURCES':<14} {grand_tickets:>8} {grand_revenue:>14.2f}")

    if target_tickets or target_revenue:
        print(f"\n  Sales targets:")
        if target_tickets:
            pct = grand_tickets / target_tickets * 100
            print(f"    Tickets: {grand_tickets} / {target_tickets} ({pct:.1f}%)")
        if target_revenue:
            pct = grand_revenue / target_revenue * 100
            print(f"    Revenue: {grand_revenue:,.2f} / {target_revenue:,.2f} EUR ({pct:.1f}%)")

    print(f"\n{'=' * 60}\n")


# ---------------------------------------------------------------------------
# Collection pipeline
# ---------------------------------------------------------------------------

def collect(*, verbose: bool = False, event_slug: str | None = None, no_telegram: bool = False) -> None:
    _setup_logging(verbose=verbose)

    events = _load_config(event_slug=event_slug)
    collected_at = datetime.now(timezone.utc).isoformat()

    db = sqlite3.connect(DB_PATH)
    try:
        _init_db(db)

        for event in events:
            name = event["name"]
            slug = event["slug"]
            sources_cfg = event.get("sources", [])

            log.info("── Event: %s (%s) ──", name, slug)
            results: list[tuple[str, str, list[dict]]] = []

            for src_cfg in sources_cfg:
                src_type = src_cfg["type"]
                src_params = src_cfg.get("params", {})
                provider_name = src_cfg.get("provider_name", src_type)

                module_path = SOURCE_MODULES.get(src_type)
                if module_path is None:
                    log.warning("Unknown source type '%s' — skipping", src_type)
                    continue

                log.info("[%s] Collecting …", provider_name)
                try:
                    mod = importlib.import_module(module_path)
                    records = mod.collect(src_params)

                    if records:
                        ins, upd = _upsert_records(db, records, slug, src_type, collected_at)
                        log.info("[%s] DB: %d inserted, %d updated, %d unchanged",
                                 provider_name, ins, upd, len(records) - ins - upd)
                    else:
                        log.info("[%s] No records returned", provider_name)
                except Exception:
                    log.exception("[%s] Collection failed", provider_name)
                    records = []

                results.append((src_type, provider_name, records))

            _print_report(event, results)

            report_path = html_report.generate(event, sources_cfg)
            log.info("HTML report: %s", report_path)

            report_url = event.get("report_base_url")
            if report_url:
                report_url = report_url.rstrip("/") + f"/{slug}.html"

            chat_id = event.get("telegram_chat_id")
            if chat_id and not no_telegram:
                msg = telegram_report.format_report(event, results, sources_cfg,
                                                    report_url=report_url)
                if msg:
                    telegram_report.send(str(chat_id), msg)
                else:
                    log.info("No data to send to Telegram for %s", slug)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Collect ticket sales data")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug output")
    parser.add_argument("-e", "--event", metavar="SLUG", help="Collect only this event (by slug)")
    parser.add_argument("--no-telegram", action="store_true", help="Skip Telegram notifications")
    args = parser.parse_args()

    try:
        collect(verbose=args.verbose, event_slug=args.event, no_telegram=args.no_telegram)
    except Exception:
        log.exception("Collection failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
