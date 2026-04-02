"""
HTML dashboard report generator.

Produces a self-contained HTML file with charts, scorecards, and analytics
for a single event, using data from all configured sources.
"""

import json
import logging
import math
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

log = logging.getLogger("collector.html")

BASE_DIR = Path(__file__).resolve().parent
REPORTS_DIR = BASE_DIR / "reports"
DB_PATH = BASE_DIR / "sales.db"


def generate(event: dict, sources_cfg: list[dict]) -> Path:
    REPORTS_DIR.mkdir(exist_ok=True)
    slug = event["slug"]
    name = event["name"]
    target = event.get("sales_target") or {}
    target_tickets = target.get("tickets") or 0
    target_revenue = target.get("revenue") or 0
    event_date_str = event.get("event_date")
    event_date = date.fromisoformat(event_date_str) if event_date_str else None

    src_cfg_map = {s["type"]: s for s in sources_cfg}
    source_types = [s["type"] for s in sources_cfg]

    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    try:
        rows = db.execute(
            "SELECT date, source, tickets, revenue_cents FROM daily_sales "
            "WHERE event_slug = ? ORDER BY date",
            (slug,),
        ).fetchall()
    finally:
        db.close()

    if not rows:
        log.warning("No data in DB for event %s — skipping HTML report", slug)
        return REPORTS_DIR / f"{slug}.html"

    by_date: dict[str, dict] = {}
    for r in rows:
        d = r["date"]
        if d not in by_date:
            by_date[d] = {}
        by_date[d][r["source"]] = {
            "tickets": r["tickets"],
            "revenue": r["revenue_cents"] / 100,
        }

    dates = sorted(by_date.keys())
    num_days = len(dates)

    per_source_data = {}
    source_totals = {}
    for src_type in source_types:
        cfg = src_cfg_map.get(src_type, {})
        provider_name = cfg.get("provider_name", src_type)
        tickets_series = []
        revenue_series = []
        for d in dates:
            vals = by_date[d].get(src_type, {"tickets": 0, "revenue": 0})
            tickets_series.append(vals["tickets"])
            revenue_series.append(vals["revenue"])
        per_source_data[src_type] = {
            "name": provider_name,
            "tickets": tickets_series,
            "revenue": revenue_series,
        }
        source_totals[src_type] = {
            "name": provider_name,
            "tickets": sum(tickets_series),
            "revenue": round(sum(revenue_series), 2),
        }

    total_tickets_series = []
    total_revenue_series = []
    cum_tickets = []
    cum_revenue = []
    running_t = 0
    running_r = 0.0
    for d in dates:
        day_t = sum(by_date[d].get(s, {}).get("tickets", 0) for s in source_types)
        day_r = sum(by_date[d].get(s, {}).get("revenue", 0) for s in source_types)
        total_tickets_series.append(day_t)
        total_revenue_series.append(day_r)
        running_t += day_t
        running_r += day_r
        cum_tickets.append(running_t)
        cum_revenue.append(round(running_r, 2))

    grand_tickets = cum_tickets[-1] if cum_tickets else 0
    grand_revenue = cum_revenue[-1] if cum_revenue else 0
    avg_daily_tickets = round(grand_tickets / num_days, 1) if num_days else 0
    avg_daily_revenue = round(grand_revenue / num_days, 2) if num_days else 0
    avg_ticket_price = round(grand_revenue / grand_tickets, 1) if grand_tickets else 0

    ma7_tickets = _moving_average(total_tickets_series, 7)
    avg_price_series = []
    for i in range(num_days):
        t = total_tickets_series[i]
        r = total_revenue_series[i]
        avg_price_series.append(round(r / t, 1) if t > 0 else None)
    ma7_price = _moving_average_nullable(avg_price_series, 7)

    last_day_tickets = total_tickets_series[-1] if total_tickets_series else 0
    prev_day_tickets = total_tickets_series[-2] if len(total_tickets_series) >= 2 else 0
    dod_tickets_pct = round((last_day_tickets - prev_day_tickets) / prev_day_tickets * 100) if prev_day_tickets else 0

    last_day_revenue = total_revenue_series[-1] if total_revenue_series else 0
    prev_day_revenue = total_revenue_series[-2] if len(total_revenue_series) >= 2 else 0
    dod_revenue_pct = round((last_day_revenue - prev_day_revenue) / prev_day_revenue * 100) if prev_day_revenue else 0

    week_data = total_tickets_series[-7:] if len(total_tickets_series) >= 7 else total_tickets_series
    week_avg_qty = round(sum(week_data) / len(week_data), 1) if week_data else 0

    week_rev_data = total_revenue_series[-7:] if len(total_revenue_series) >= 7 else total_revenue_series
    week_tix_data = total_tickets_series[-7:] if len(total_tickets_series) >= 7 else total_tickets_series
    week_total_rev = sum(week_rev_data)
    week_total_tix = sum(week_tix_data)
    week_avg_price = round(week_total_rev / week_total_tix, 1) if week_total_tix else 0

    forecast_rate = _trimmed_mean_rate(total_tickets_series)

    days_remaining = (event_date - date.today()).days if event_date else None

    if target_tickets and forecast_rate > 0:
        remaining_tickets = target_tickets - grand_tickets
        if remaining_tickets <= 0:
            forecast_days_to_target = 0
            forecast_target_date = date.today().isoformat()
        else:
            forecast_days_to_target = math.ceil(remaining_tickets / forecast_rate)
            forecast_target_date = (date.today() + timedelta(days=forecast_days_to_target)).isoformat()
    else:
        forecast_days_to_target = None
        forecast_target_date = None

    projected_total = round(grand_tickets + forecast_rate * days_remaining) if (days_remaining and forecast_rate) else None

    projection_line = None
    if forecast_rate and days_remaining and days_remaining > 0:
        proj_dates = []
        proj_values = []
        today = date.today()
        current_cum = grand_tickets
        for i in range(days_remaining + 1):
            d = today + timedelta(days=i)
            proj_dates.append(d.isoformat())
            proj_values.append(round(current_cum + forecast_rate * i))
        projection_line = {"dates": proj_dates, "values": proj_values}

    chart_data = {
        "dates": dates,
        "sources": per_source_data,
        "totalTickets": total_tickets_series,
        "totalRevenue": total_revenue_series,
        "cumTickets": cum_tickets,
        "cumRevenue": cum_revenue,
        "ma7Tickets": ma7_tickets,
        "avgPrice": avg_price_series,
        "ma7Price": ma7_price,
        "targetTickets": target_tickets,
        "targetRevenue": target_revenue,
        "sourceTotals": source_totals,
        "projection": projection_line,
    }

    scorecards = {
        "grandTickets": grand_tickets,
        "grandRevenue": round(grand_revenue),
        "targetTickets": target_tickets,
        "targetRevenue": target_revenue,
        "avgDailyQty": avg_daily_tickets,
        "avgTicketPrice": avg_ticket_price,
        "numDays": num_days,
        "lastDayTickets": last_day_tickets,
        "lastDayRevenue": round(last_day_revenue),
        "dodTicketsPct": dod_tickets_pct,
        "dodRevenuePct": dod_revenue_pct,
        "weekAvgQty": week_avg_qty,
        "weekAvgPrice": week_avg_price,
        "forecastRate": round(forecast_rate, 1) if forecast_rate else None,
        "forecastDaysToTarget": forecast_days_to_target,
        "forecastTargetDate": forecast_target_date,
        "projectedTotal": projected_total,
        "eventDate": event_date.isoformat() if event_date else None,
        "daysRemaining": days_remaining,
    }

    html = _render_html(name, chart_data, scorecards, sources_cfg)
    out_path = REPORTS_DIR / f"{slug}.html"
    out_path.write_text(html, encoding="utf-8")
    log.info("HTML report generated: %s", out_path)
    return out_path


def _moving_average(data: list[int], window: int) -> list[float | None]:
    result = []
    for i in range(len(data)):
        if i < window - 1:
            result.append(None)
        else:
            avg = sum(data[i - window + 1 : i + 1]) / window
            result.append(round(avg, 1))
    return result


def _moving_average_nullable(data: list[float | None], window: int) -> list[float | None]:
    result = []
    for i in range(len(data)):
        if i < window - 1:
            result.append(None)
            continue
        segment = [v for v in data[i - window + 1 : i + 1] if v is not None]
        if len(segment) >= window // 2:
            result.append(round(sum(segment) / len(segment), 1))
        else:
            result.append(None)
    return result


def _trimmed_mean_rate(daily_tickets: list[int], recent_days: int = 14, trim_pct: float = 0.15) -> float:
    """
    Compute a robust daily sales rate using a trimmed mean.

    Takes the last `recent_days` of data (or all data if fewer),
    sorts values, trims the top and bottom `trim_pct`, and averages
    the middle portion.  This removes anomalies like launch-day
    spikes or unusual dips.
    """
    if not daily_tickets:
        return 0.0
    window = daily_tickets[-recent_days:]
    n = len(window)
    if n < 3:
        return sum(window) / n

    trim_count = max(1, int(n * trim_pct))
    sorted_vals = sorted(window)
    trimmed = sorted_vals[trim_count : n - trim_count]
    if not trimmed:
        trimmed = sorted_vals
    return sum(trimmed) / len(trimmed)


def _render_html(
    event_name: str,
    chart_data: dict,
    scorecards: dict,
    sources_cfg: list[dict],
) -> str:
    generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    data_json = json.dumps(chart_data, ensure_ascii=False)
    scores_json = json.dumps(scorecards, ensure_ascii=False)
    sources_json = json.dumps(
        [{"type": s["type"], "name": s.get("provider_name", s["type"]),
          "link": s.get("provider_link", ""), "url": s.get("event_page_url", "")}
         for s in sources_cfg],
        ensure_ascii=False,
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{event_name} — Sales Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
:root {{
  --bg: #0f1117;
  --surface: #1a1d27;
  --border: #2a2d3a;
  --text: #e4e4e7;
  --text-muted: #8b8d98;
  --accent: #6366f1;
  --accent2: #22d3ee;
  --green: #22c55e;
  --orange: #f59e0b;
  --red: #ef4444;
  --pink: #ec4899;
}}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  background: var(--bg);
  color: var(--text);
  padding: 24px;
  max-width: 1320px;
  margin: 0 auto;
}}
h1 {{ font-size: 28px; font-weight: 700; margin-bottom: 4px; }}
.subtitle {{ color: var(--text-muted); font-size: 13px; margin-bottom: 24px; }}
.card-row {{
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 12px;
  margin-bottom: 12px;
}}
.card {{
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 16px;
}}
.card .label {{
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  color: var(--text-muted);
  margin-bottom: 6px;
}}
.card .value {{
  font-size: 24px;
  font-weight: 700;
  white-space: nowrap;
}}
.card .sub {{
  font-size: 12px;
  color: var(--text-muted);
  margin-top: 4px;
}}
.up {{ color: var(--green) !important; }}
.down {{ color: var(--red) !important; }}
.neutral {{ color: var(--text-muted) !important; }}
.progress-wrap {{
  margin-top: 8px;
  height: 6px;
  background: var(--border);
  border-radius: 3px;
  overflow: hidden;
}}
.progress-bar {{
  height: 100%;
  border-radius: 3px;
  transition: width 0.6s ease;
}}
.section-label {{
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 1px;
  color: var(--text-muted);
  margin: 20px 0 8px 4px;
}}
.charts {{
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(540px, 1fr));
  gap: 16px;
  margin-bottom: 24px;
}}
.chart-box {{
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 20px;
}}
.chart-box.full {{ grid-column: 1 / -1; }}
.chart-box h3 {{ font-size: 14px; font-weight: 600; margin-bottom: 12px; color: var(--text-muted); }}
.chart-box canvas {{ width: 100% !important; }}
.table-wrap {{
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 20px;
  overflow-x: auto;
  margin-bottom: 24px;
}}
.table-wrap h3 {{ font-size: 14px; font-weight: 600; margin-bottom: 12px; color: var(--text-muted); }}
table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
th, td {{ text-align: right; padding: 8px 12px; border-bottom: 1px solid var(--border); }}
th {{ color: var(--text-muted); font-weight: 500; font-size: 11px; text-transform: uppercase; }}
td:first-child, th:first-child {{ text-align: left; }}
tr:hover td {{ background: rgba(99,102,241,0.06); }}
.sources-row {{
  display: flex; gap: 16px; margin-bottom: 20px; flex-wrap: wrap;
}}
.source-badge {{
  display: inline-flex; align-items: center; gap: 6px;
  background: var(--surface); border: 1px solid var(--border);
  border-radius: 8px; padding: 6px 14px; font-size: 13px;
  text-decoration: none; color: var(--text);
}}
.source-badge:hover {{ border-color: var(--accent); }}
.dot {{ width:8px; height:8px; border-radius:50%; display:inline-block; }}
.forecast-box {{
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 20px;
  margin-bottom: 24px;
}}
.forecast-grid {{
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 16px;
}}
.forecast-item .fi-label {{ font-size: 11px; color: var(--text-muted); text-transform: uppercase; margin-bottom: 4px; }}
.forecast-item .fi-value {{ font-size: 20px; font-weight: 700; }}
.forecast-item .fi-sub {{ font-size: 12px; color: var(--text-muted); margin-top: 2px; }}
@media (max-width: 900px) {{
  .charts {{ grid-template-columns: 1fr; }}
  .card-row {{ grid-template-columns: repeat(2, 1fr); }}
  body {{ padding: 12px; }}
}}
@media (max-width: 500px) {{
  .card-row {{ grid-template-columns: 1fr; }}
}}
</style>
</head>
<body>

<h1>{event_name}</h1>
<div class="subtitle">Sales Dashboard — updated {generated_at}</div>

<div class="sources-row" id="sourceBadges"></div>

<div class="section-label">Cumulative</div>
<div class="card-row" id="row1"></div>

<div class="section-label">Yesterday</div>
<div class="card-row" id="row2"></div>

<div class="section-label">Forecast</div>
<div id="forecastSection"></div>

<div class="section-label">Charts</div>
<div class="charts">
  <div class="chart-box"><h3>Daily Tickets by Source + 7d MA</h3><canvas id="chartDailyTickets"></canvas></div>
  <div class="chart-box"><h3>Cumulative Tickets vs Target</h3><canvas id="chartCumTickets"></canvas></div>
  <div class="chart-box"><h3>Cumulative Revenue vs Target</h3><canvas id="chartCumRevenue"></canvas></div>
  <div class="chart-box"><h3>Average Ticket Price + 7d Trend</h3><canvas id="chartAvgPrice"></canvas></div>
  <div class="chart-box"><h3>Week-over-Week Qty Change</h3><canvas id="chartWoW"></canvas></div>
  <div class="chart-box"><h3>Sales by Provider</h3><canvas id="chartProviders"></canvas></div>
</div>

<div class="table-wrap">
  <h3>Daily Breakdown</h3>
  <table id="detailTable"></table>
</div>

<script>
const D = {data_json};
const S = {scores_json};
const SRC = {sources_json};

const COLORS = ['#6366f1','#22d3ee','#f59e0b','#ec4899','#22c55e','#f97316'];
const fmtN = n => n.toLocaleString('en-US').replace(/,/g, ' ');
const fmtC = (n, d=2) => n.toLocaleString('en-US', {{minimumFractionDigits:d, maximumFractionDigits:d}}).replace(/,/g, ' ');
const fmtC1 = n => fmtC(n, 1);
const fmtInt = n => Math.round(n).toLocaleString('en-US').replace(/,/g, ' ');
const pct = (a,b) => b ? ((a/b)*100).toFixed(1)+'%' : '—';

// Source badges
const badgesEl = document.getElementById('sourceBadges');
SRC.forEach((s,i) => {{
  const a = document.createElement(s.url ? 'a' : 'span');
  a.className = 'source-badge';
  if (s.url) {{ a.href = s.url; a.target = '_blank'; }}
  a.innerHTML = `<span class="dot" style="background:${{COLORS[i%COLORS.length]}}"></span>${{s.name}}`;
  badgesEl.appendChild(a);
}});

// Card helper
function addCard(container, label, value, sub, progressPct) {{
  const d = document.createElement('div');
  d.className = 'card';
  let html = `<div class="label">${{label}}</div><div class="value">${{value}}</div>`;
  if (sub) html += `<div class="sub">${{sub}}</div>`;
  if (progressPct !== undefined) {{
    const color = progressPct >= 75 ? '#22c55e' : progressPct >= 40 ? '#f59e0b' : '#6366f1';
    html += `<div class="progress-wrap"><div class="progress-bar" style="width:${{Math.min(progressPct,100)}}%;background:${{color}}"></div></div>`;
  }}
  d.innerHTML = html;
  container.appendChild(d);
}}

// Row 1: Cumulative
const row1 = document.getElementById('row1');
addCard(row1, 'Total Tickets', fmtN(S.grandTickets),
  S.targetTickets ? `${{pct(S.grandTickets, S.targetTickets)}} of ${{fmtN(S.targetTickets)}} target` : `${{S.numDays}} days of sales`,
  S.targetTickets ? (S.grandTickets/S.targetTickets*100) : undefined);

addCard(row1, 'Total Revenue', fmtInt(S.grandRevenue) + ' €',
  S.targetRevenue ? `${{pct(S.grandRevenue, S.targetRevenue)}} of ${{fmtInt(S.targetRevenue)}} € target` : null,
  S.targetRevenue ? (S.grandRevenue/S.targetRevenue*100) : undefined);

addCard(row1, 'Avg Ticket Price', fmtC1(S.avgTicketPrice) + ' €', null);

addCard(row1, 'Daily Average Qty', fmtC1(S.avgDailyQty), `${{S.numDays}} days of sales`);

// Row 2: Yesterday
const row2 = document.getElementById('row2');
const dodTClass = S.dodTicketsPct > 0 ? 'up' : S.dodTicketsPct < 0 ? 'down' : 'neutral';
const dodTSign = S.dodTicketsPct > 0 ? '+' : '';
addCard(row2, 'Yesterday Qty', fmtN(S.lastDayTickets),
  `<span class="${{dodTClass}}">${{dodTSign}}${{S.dodTicketsPct}}% vs prev day</span>`);

const dodRClass = S.dodRevenuePct > 0 ? 'up' : S.dodRevenuePct < 0 ? 'down' : 'neutral';
const dodRSign = S.dodRevenuePct > 0 ? '+' : '';
addCard(row2, 'Yesterday Revenue', fmtInt(S.lastDayRevenue) + ' €',
  `<span class="${{dodRClass}}">${{dodRSign}}${{S.dodRevenuePct}}% vs prev day</span>`);

addCard(row2, 'Week Avg Price', fmtC1(S.weekAvgPrice) + ' €', 'last 7 days');
addCard(row2, 'Week Avg Qty', fmtC1(S.weekAvgQty), 'last 7 days');

// Forecast section
const forecastEl = document.getElementById('forecastSection');
if (S.forecastRate || S.projectedTotal || S.daysRemaining !== null) {{
  let fHtml = '<div class="forecast-box"><div class="forecast-grid">';
  if (S.forecastRate) {{
    fHtml += `<div class="forecast-item"><div class="fi-label">Steady-state Rate</div><div class="fi-value">${{fmtC1(S.forecastRate)}} / day</div><div class="fi-sub">trimmed mean, last 14d</div></div>`;
  }}
  if (S.projectedTotal) {{
    fHtml += `<div class="forecast-item"><div class="fi-label">Projected Total by Event</div><div class="fi-value">${{fmtN(S.projectedTotal)}}</div><div class="fi-sub">${{S.eventDate}} · ${{S.daysRemaining}} days left</div></div>`;
  }}
  if (S.forecastDaysToTarget !== null && S.forecastDaysToTarget > 0) {{
    fHtml += `<div class="forecast-item"><div class="fi-label">Target Date (est.)</div><div class="fi-value">${{S.forecastTargetDate}}</div><div class="fi-sub">${{S.forecastDaysToTarget}} days at current pace</div></div>`;
  }} else if (S.forecastDaysToTarget !== null && S.forecastDaysToTarget <= 0) {{
    fHtml += `<div class="forecast-item"><div class="fi-label">Target</div><div class="fi-value">✅ Reached</div></div>`;
  }}
  fHtml += '</div></div>';
  forecastEl.innerHTML = fHtml;
}}

// Chart defaults
Chart.defaults.color = '#8b8d98';
Chart.defaults.borderColor = '#2a2d3a';
Chart.defaults.font.family = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif";
Chart.defaults.font.size = 11;

const shortDates = D.dates.map(d => d.slice(5));

// 1. Daily tickets (stacked bar + MA7 line)
new Chart(document.getElementById('chartDailyTickets'), {{
  data: {{
    labels: shortDates,
    datasets: [
      ...Object.entries(D.sources).map(([k,v], i) => ({{
        type: 'bar',
        label: v.name,
        data: v.tickets,
        backgroundColor: COLORS[i%COLORS.length] + 'cc',
        borderRadius: 3,
        stack: 'stack0',
        order: 2,
      }})),
      {{
        type: 'line',
        label: '7-day MA',
        data: D.ma7Tickets,
        borderColor: '#ec4899',
        borderWidth: 2.5,
        pointRadius: 0,
        tension: 0.4,
        spanGaps: true,
        order: 1,
      }}
    ]
  }},
  options: {{
    responsive: true,
    plugins: {{ legend: {{ position:'top' }} }},
    scales: {{
      x: {{ stacked: true }},
      y: {{ stacked: true, beginAtZero: true }}
    }}
  }}
}});

// 2. Cumulative tickets + projection
(() => {{
  const datasets = [
    {{
      label: 'Cumulative Tickets',
      data: D.cumTickets,
      borderColor: '#6366f1',
      backgroundColor: 'rgba(99,102,241,0.1)',
      fill: true, tension: 0.3, pointRadius: 2,
    }},
  ];
  if (D.targetTickets) {{
    datasets.push({{
      label: 'Target (' + fmtN(D.targetTickets) + ')',
      data: D.dates.map(() => D.targetTickets),
      borderColor: '#22c55e', borderDash: [6,4], pointRadius: 0, borderWidth: 2,
    }});
  }}
  let labels = [...shortDates];
  if (D.projection) {{
    const projDs = new Array(D.dates.length).fill(null);
    projDs[projDs.length - 1] = D.cumTickets[D.cumTickets.length - 1];
    const projLabels = D.projection.dates.map(d => d.slice(5));
    for (let i = 1; i < D.projection.dates.length; i++) {{
      labels.push(projLabels[i]);
      projDs.push(D.projection.values[i]);
      datasets.forEach(ds => {{ if (ds.label !== 'Projection') ds.data.push(null); }});
      if (D.targetTickets) {{
        datasets[1].data[datasets[1].data.length - 1] = D.targetTickets;
      }}
    }}
    datasets.push({{
      label: 'Projection',
      data: projDs,
      borderColor: '#f59e0b',
      borderDash: [4,3],
      pointRadius: 0,
      borderWidth: 2,
      spanGaps: true,
    }});
  }}
  new Chart(document.getElementById('chartCumTickets'), {{
    type: 'line',
    data: {{ labels, datasets }},
    options: {{ responsive: true, plugins: {{ legend: {{ position:'top' }} }}, scales: {{ y: {{ beginAtZero: true }} }} }}
  }});
}})();

// 3. Cumulative revenue
new Chart(document.getElementById('chartCumRevenue'), {{
  type: 'line',
  data: {{
    labels: shortDates,
    datasets: [
      {{
        label: 'Cumulative Revenue (€)',
        data: D.cumRevenue,
        borderColor: '#22d3ee',
        backgroundColor: 'rgba(34,211,238,0.1)',
        fill: true, tension: 0.3, pointRadius: 2,
      }},
      ...(D.targetRevenue ? [{{
        label: 'Target (' + fmtN(D.targetRevenue) + ' €)',
        data: D.dates.map(() => D.targetRevenue),
        borderColor: '#22c55e', borderDash: [6,4], pointRadius: 0, borderWidth: 2,
      }}] : [])
    ]
  }},
  options: {{ responsive: true, plugins: {{ legend: {{ position:'top' }} }}, scales: {{ y: {{ beginAtZero: true }} }} }}
}});

// 4. Avg price + MA7 trend
new Chart(document.getElementById('chartAvgPrice'), {{
  type: 'line',
  data: {{
    labels: shortDates,
    datasets: [
      {{
        label: 'Daily Avg Price (€)',
        data: D.avgPrice,
        borderColor: 'rgba(245,158,11,0.4)',
        pointRadius: 3,
        pointBackgroundColor: '#f59e0b',
        borderWidth: 1,
        tension: 0.2,
      }},
      {{
        label: '7-day MA',
        data: D.ma7Price,
        borderColor: '#f59e0b',
        borderWidth: 2.5,
        pointRadius: 0,
        tension: 0.4,
        spanGaps: true,
      }}
    ]
  }},
  options: {{ responsive: true, plugins: {{ legend: {{ position:'top' }} }}, scales: {{ y: {{ beginAtZero: false }} }} }}
}});

// 5. Week-over-Week growth
(() => {{
  const wowLabels = [];
  const wowData = [];
  for (let i = 7; i < D.totalTickets.length; i++) {{
    const prev7 = D.totalTickets.slice(i-7, i).reduce((a,b)=>a+b, 0);
    const cur7 = D.totalTickets.slice(Math.max(0,i-14), i-7).reduce((a,b)=>a+b, 0);
    const changePct = cur7 > 0 ? ((prev7 - cur7) / cur7 * 100) : 0;
    wowLabels.push(shortDates[i]);
    wowData.push(Math.round(changePct * 10) / 10);
  }}
  const bgColors = wowData.map(v => v >= 0 ? 'rgba(34,197,94,0.7)' : 'rgba(239,68,68,0.7)');
  new Chart(document.getElementById('chartWoW'), {{
    type: 'bar',
    data: {{
      labels: wowLabels,
      datasets: [{{
        label: 'WoW Change %',
        data: wowData,
        backgroundColor: bgColors,
        borderRadius: 3,
      }}]
    }},
    options: {{
      responsive: true,
      plugins: {{ legend: {{ display: false }} }},
      scales: {{ y: {{ beginAtZero: true,
        ticks: {{ callback: v => v + '%' }}
      }} }}
    }}
  }});
}})();

// 6. Provider distribution (doughnut)
(() => {{
  const srcKeys = Object.keys(D.sourceTotals);
  const labels = srcKeys.map(k => D.sourceTotals[k].name);
  const tixData = srcKeys.map(k => D.sourceTotals[k].tickets);
  new Chart(document.getElementById('chartProviders'), {{
    type: 'doughnut',
    data: {{
      labels,
      datasets: [{{
        data: tixData,
        backgroundColor: srcKeys.map((k,i) => COLORS[i%COLORS.length] + 'cc'),
        borderColor: '#1a1d27',
        borderWidth: 3,
      }}]
    }},
    options: {{
      responsive: true,
      plugins: {{
        legend: {{ position: 'bottom' }},
        tooltip: {{
          callbacks: {{
            label: ctx => {{
              const total = ctx.dataset.data.reduce((a,b)=>a+b,0);
              const val = ctx.parsed;
              const pctV = ((val/total)*100).toFixed(1);
              return `${{ctx.label}}: ${{fmtN(val)}} (${{pctV}}%)`;
            }}
          }}
        }}
      }}
    }}
  }});
}})();

// Detail table
const tableEl = document.getElementById('detailTable');
const srcKeys = Object.keys(D.sources);
let thead = '<thead><tr><th>Date</th>';
srcKeys.forEach(k => {{
  thead += `<th>${{D.sources[k].name}} qty</th><th>${{D.sources[k].name}} €</th>`;
}});
thead += '<th>Total qty</th><th>Total €</th><th>Avg €</th><th>Cum qty</th></tr></thead>';

let tbody = '<tbody>';
for (let i = 0; i < D.dates.length; i++) {{
  tbody += `<tr><td>${{D.dates[i]}}</td>`;
  srcKeys.forEach(k => {{
    tbody += `<td>${{fmtN(D.sources[k].tickets[i])}}</td><td>${{fmtC(D.sources[k].revenue[i])}}</td>`;
  }});
  tbody += `<td><b>${{fmtN(D.totalTickets[i])}}</b></td>`;
  tbody += `<td>${{fmtC(D.totalRevenue[i])}}</td>`;
  tbody += `<td>${{D.avgPrice[i] !== null ? fmtC1(D.avgPrice[i]) : '—'}}</td>`;
  tbody += `<td>${{fmtN(D.cumTickets[i])}}</td>`;
  tbody += '</tr>';
}}
tbody += '</tbody>';
tableEl.innerHTML = thead + tbody;
</script>
</body>
</html>"""
