"""
mticket source — collects daily ticket sales from the mticket MySQL database.

Requires env vars: MTICKET_DB_HOST, MTICKET_DB_PORT, MTICKET_DB_USER,
                   MTICKET_DB_PASSWORD, MTICKET_DB_NAME
Per-event params (from config.yaml): event_id
"""

import logging
import os
from datetime import date, timedelta

log = logging.getLogger("collector.mticket")


def collect(params: dict) -> list[dict]:
    """
    Fetch daily sales from the mticket report.data table.

    params must contain: event_id
    """
    try:
        import mysql.connector
    except ImportError:
        log.error("mysql-connector-python is not installed. Run: pip install mysql-connector-python")
        raise

    host = os.environ["MTICKET_DB_HOST"]
    port = int(os.getenv("MTICKET_DB_PORT", "3306"))
    user = os.environ["MTICKET_DB_USER"]
    password = os.environ["MTICKET_DB_PASSWORD"]
    database = os.environ["MTICKET_DB_NAME"]
    event_id = params["event_id"]
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    log.info("Connecting to %s:%d/%s …", host, port, database)

    conn = mysql.connector.connect(
        host=host, port=port, user=user, password=password, database=database,
        connect_timeout=30,
    )
    try:
        cursor = conn.cursor()
        query = """
            SELECT
                DATE(Date)            AS sale_date,
                SUM(TicketsQtty)      AS tickets,
                SUM(FaceValueAmount)  AS revenue_eur
            FROM report.data
            WHERE EventID = %s
              AND DATE(Date) <= %s
            GROUP BY DATE(Date)
            ORDER BY sale_date
        """
        cursor.execute(query, (event_id, yesterday))
        rows = cursor.fetchall()
    finally:
        conn.close()

    if not rows:
        log.warning("No rows returned for event %s", event_id)
        return []

    records = []
    for sale_date, tickets, revenue in rows:
        records.append({
            "date": sale_date.isoformat() if hasattr(sale_date, "isoformat") else str(sale_date),
            "tickets": int(tickets),
            "revenue_eur": round(float(revenue), 2),
        })

    log.info("Fetched %d records: %s → %s", len(records), records[0]["date"], records[-1]["date"])
    return records
