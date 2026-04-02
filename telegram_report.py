"""
Telegram daily report formatter and sender.
"""

import logging
import os
from urllib.parse import urlparse

import requests

log = logging.getLogger("collector.telegram")

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


def _fmt_number(n: int | float) -> str:
    """Format number with space as thousands separator: 139543 → '139 543'."""
    if isinstance(n, float):
        return f"{n:,.0f}".replace(",", " ")
    return f"{n:,}".replace(",", " ")


def _delta_str(today_val: int, yesterday_val: int) -> str:
    """Format day-over-day change: '(+25%⬆︎)' or '(-33%⬇︎)' or '(+0%)'."""
    if yesterday_val == 0:
        return "(new)" if today_val > 0 else "(+0%)"
    pct = (today_val - yesterday_val) / yesterday_val * 100
    if pct > 0:
        return f"(+{pct:.0f}%⬆︎)"
    elif pct < 0:
        return f"({pct:.0f}%⬇︎)"
    return "(+0%)"


def _progress_bar(current: int, target: int, width: int = 10) -> str:
    """Render emoji progress bar: 🟧🟧🟧🟨⬜️⬜️⬜️⬜️⬜️⬜️ 38.7%"""
    if target <= 0:
        return ""
    ratio = min(current / target, 1.0)
    pct = ratio * 100
    filled = int(ratio * width)
    partial = 1 if (ratio * width - filled) >= 0.5 and filled < width else 0
    empty = width - filled - partial

    bar = "🟧" * filled + "🟨" * partial + "⬜️" * empty
    return f"{bar} {pct:.1f}%"


def format_report(
    event: dict,
    results: list[tuple[str, str, list[dict]]],
    sources_cfg: list[dict],
    *,
    report_url: str | None = None,
) -> str | None:
    """
    Build the Telegram message text for one event.

    Returns None if there's no data to report.
    """
    target = event.get("sales_target") or {}
    target_tickets = target.get("tickets")

    lines: list[str] = []
    grand_tickets = 0
    grand_revenue = 0.0
    grand_last_day = 0
    grand_prev_day = 0

    src_cfg_map = {s["type"]: s for s in sources_cfg}

    for src_type, _provider_name, records in results:
        if not records:
            continue

        cfg = src_cfg_map.get(src_type, {})
        provider_link = cfg.get("provider_link", "")
        event_page_url = cfg.get("event_page_url", "")
        if provider_link:
            parsed = urlparse(provider_link if "://" in provider_link else f"https://{provider_link}")
            display_domain = parsed.netloc or provider_link
        else:
            display_domain = src_type

        total_tickets = sum(r["tickets"] for r in records)
        total_revenue = sum(r["revenue_eur"] for r in records)
        grand_tickets += total_tickets
        grand_revenue += total_revenue

        last_day_tickets = records[-1]["tickets"] if records else 0
        prev_day_tickets = records[-2]["tickets"] if len(records) >= 2 else 0
        grand_last_day += last_day_tickets
        grand_prev_day += prev_day_tickets

        delta = _delta_str(last_day_tickets, prev_day_tickets)

        if event_page_url:
            header = f'🎟️ <a href="{event_page_url}">{display_domain}</a>'
        else:
            header = f"🎟️ {display_domain}"

        lines.append(header)
        lines.append(f"Tickets: {_fmt_number(total_tickets)} • <b>{last_day_tickets}</b> {delta}")
        lines.append(f"Revenue: {_fmt_number(total_revenue)} €")
        lines.append("")

    if not lines:
        return None

    grand_delta = _delta_str(grand_last_day, grand_prev_day)

    lines.append("✅ TOTAL")
    lines.append(f"Tickets: {_fmt_number(grand_tickets)} • <b>{grand_last_day}</b> {grand_delta}")
    lines.append(f"Revenue:   {_fmt_number(grand_revenue)} €")

    if target_tickets and target_tickets > 0:
        lines.append("")
        bar = _progress_bar(grand_tickets, target_tickets)
        lines.append(bar)
        lines.append(f"/ {_fmt_number(target_tickets)}")

    if report_url:
        lines.append("")
        lines.append(f'📊 <a href="{report_url}">Full Dashboard</a>')

    return "\n".join(lines)


def send(chat_id: str, text: str) -> bool:
    """Send a message via Telegram Bot API. Returns True on success."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        log.error("TELEGRAM_BOT_TOKEN not set — cannot send")
        return False

    url = TELEGRAM_API.format(token=token)
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        resp = requests.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        log.info("Telegram message sent to %s", chat_id)
        return True
    except requests.RequestException as exc:
        log.error("Telegram send failed: %s", exc)
        return False
