"""
app.py — Florida Sunbiz Compliance Control Center
Single-file Streamlit dashboard for managing the Annual Report reminder pipeline.

Run: streamlit run app.py
"""

import os
import re
import csv
import json
import time
import stat
import logging
import threading
import queue
from io import StringIO
from datetime import datetime
from dataclasses import dataclass, field, asdict
from pathlib import Path
from textwrap import dedent
from typing import Optional

import streamlit as st
import pandas as pd

# ── Page Config (must be first Streamlit call) ─────────────────────────────
st.set_page_config(
    page_title="Sunbiz Control Center",
    page_icon="🏛",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ══════════════════════════════════════════════════════════════════════════════
# DESIGN SYSTEM — Custom CSS
# Aesthetic: Dark government-grade precision. Deep navy + amber accent.
# Feels like a Bloomberg terminal crossed with a federal compliance portal.
# ══════════════════════════════════════════════════════════════════════════════

CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

/* ── Root Variables ─────────────────────────────────────────── */
:root {
    --navy:      #0a1628;
    --navy-mid:  #0f2040;
    --navy-card: #132038;
    --navy-border: #1e3a5f;
    --amber:     #f59e0b;
    --amber-dim: #b45309;
    --green:     #10b981;
    --red:       #ef4444;
    --muted:     #64748b;
    --text:      #e2e8f0;
    --text-dim:  #94a3b8;
    --mono:      'IBM Plex Mono', monospace;
    --sans:      'IBM Plex Sans', sans-serif;
}

/* ── Global Reset ───────────────────────────────────────────── */
html, body, [class*="css"] {
    font-family: var(--sans) !important;
    background-color: var(--navy) !important;
    color: var(--text) !important;
}

.main .block-container {
    padding: 1.5rem 2rem 3rem 2rem;
    max-width: 1400px;
}

/* ── Scrollbar ──────────────────────────────────────────────── */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: var(--navy); }
::-webkit-scrollbar-thumb { background: var(--navy-border); border-radius: 3px; }

/* ── Sidebar ────────────────────────────────────────────────── */
[data-testid="stSidebar"] {
    background-color: var(--navy-mid) !important;
    border-right: 1px solid var(--navy-border) !important;
}
[data-testid="stSidebar"] * { font-family: var(--sans) !important; }
[data-testid="stSidebar"] .stTextInput input,
[data-testid="stSidebar"] .stSelectbox select {
    background-color: var(--navy) !important;
    border: 1px solid var(--navy-border) !important;
    color: var(--text) !important;
    font-family: var(--mono) !important;
    font-size: 12px !important;
}
[data-testid="stSidebar"] label {
    color: var(--text-dim) !important;
    font-size: 11px !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
    font-weight: 500 !important;
}

/* ── Header ─────────────────────────────────────────────────── */
.dash-header {
    display: flex;
    align-items: center;
    gap: 16px;
    padding: 0 0 1.5rem 0;
    border-bottom: 1px solid var(--navy-border);
    margin-bottom: 1.5rem;
}
.dash-header-badge {
    background: var(--amber);
    color: var(--navy);
    font-family: var(--mono);
    font-size: 10px;
    font-weight: 600;
    padding: 2px 8px;
    border-radius: 2px;
    letter-spacing: 0.1em;
}
.dash-title {
    font-family: var(--mono) !important;
    font-size: 22px !important;
    font-weight: 600 !important;
    color: var(--text) !important;
    letter-spacing: -0.02em;
    margin: 0 !important;
}
.dash-subtitle {
    font-size: 12px;
    color: var(--text-dim);
    font-family: var(--mono);
    margin-top: 2px;
}

/* ── KPI Cards ──────────────────────────────────────────────── */
.kpi-row {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 12px;
    margin-bottom: 1.5rem;
}
.kpi-card {
    background: var(--navy-card);
    border: 1px solid var(--navy-border);
    border-radius: 6px;
    padding: 16px 20px;
    position: relative;
    overflow: hidden;
}
.kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: var(--amber);
}
.kpi-card.green::before { background: var(--green); }
.kpi-card.red::before { background: var(--red); }
.kpi-card.muted::before { background: var(--muted); }
.kpi-label {
    font-family: var(--mono);
    font-size: 10px;
    color: var(--text-dim);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 8px;
}
.kpi-value {
    font-family: var(--mono);
    font-size: 32px;
    font-weight: 600;
    color: var(--text);
    line-height: 1;
}
.kpi-sub {
    font-size: 11px;
    color: var(--text-dim);
    margin-top: 4px;
}

/* ── Section Headers ────────────────────────────────────────── */
.section-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 12px;
}
.section-title {
    font-family: var(--mono);
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    color: var(--text-dim);
    font-weight: 600;
}
.section-divider {
    flex: 1;
    height: 1px;
    background: var(--navy-border);
    margin-left: 16px;
}

/* ── Action Button ──────────────────────────────────────────── */
.stButton button {
    background: var(--amber) !important;
    color: var(--navy) !important;
    border: none !important;
    font-family: var(--mono) !important;
    font-size: 12px !important;
    font-weight: 600 !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase !important;
    padding: 10px 24px !important;
    border-radius: 4px !important;
    transition: all 0.15s ease !important;
}
.stButton button:hover {
    background: #fbbf24 !important;
    transform: translateY(-1px) !important;
    box-shadow: 0 4px 20px rgba(245,158,11,0.3) !important;
}
.stButton button:active {
    transform: translateY(0) !important;
}

/* Secondary buttons */
.btn-secondary button {
    background: transparent !important;
    color: var(--text-dim) !important;
    border: 1px solid var(--navy-border) !important;
    font-size: 11px !important;
    padding: 6px 14px !important;
}
.btn-secondary button:hover {
    border-color: var(--amber) !important;
    color: var(--amber) !important;
    background: transparent !important;
    transform: none !important;
    box-shadow: none !important;
}

/* ── Status Badges ──────────────────────────────────────────── */
.badge {
    display: inline-block;
    font-family: var(--mono);
    font-size: 10px;
    font-weight: 600;
    padding: 2px 8px;
    border-radius: 2px;
    text-transform: uppercase;
    letter-spacing: 0.06em;
}
.badge-pending  { background: rgba(245,158,11,0.15); color: var(--amber); border: 1px solid rgba(245,158,11,0.3); }
.badge-contacted { background: rgba(16,185,129,0.12); color: var(--green); border: 1px solid rgba(16,185,129,0.25); }
.badge-paid     { background: rgba(16,185,129,0.2); color: #34d399; border: 1px solid rgba(52,211,153,0.3); }
.badge-skipped  { background: rgba(100,116,139,0.15); color: var(--muted); border: 1px solid rgba(100,116,139,0.2); }

/* ── Log Console ────────────────────────────────────────────── */
.log-console {
    background: #050e1a;
    border: 1px solid var(--navy-border);
    border-radius: 6px;
    padding: 14px 16px;
    font-family: var(--mono);
    font-size: 11px;
    color: #7dd3fc;
    height: 180px;
    overflow-y: auto;
    line-height: 1.7;
}
.log-err  { color: #f87171; }
.log-warn { color: var(--amber); }
.log-ok   { color: var(--green); }
.log-info { color: #7dd3fc; }

/* ── Email Previewer ────────────────────────────────────────── */
.email-preview-wrap {
    background: var(--navy-card);
    border: 1px solid var(--navy-border);
    border-radius: 6px;
    padding: 0;
    overflow: hidden;
}
.email-preview-header {
    background: var(--navy-mid);
    border-bottom: 1px solid var(--navy-border);
    padding: 12px 16px;
}
.email-preview-field {
    font-family: var(--mono);
    font-size: 11px;
    color: var(--text-dim);
    margin-bottom: 3px;
}
.email-preview-field span {
    color: var(--text);
}
.email-preview-body {
    padding: 16px;
    font-family: var(--mono);
    font-size: 11.5px;
    line-height: 1.75;
    color: #c5d5e8;
    white-space: pre-wrap;
    max-height: 420px;
    overflow-y: auto;
}

/* ── Dataframe overrides ────────────────────────────────────── */
.stDataFrame {
    border: 1px solid var(--navy-border) !important;
    border-radius: 6px !important;
    overflow: hidden;
}
[data-testid="stDataFrame"] iframe {
    border-radius: 6px;
}

/* ── Text inputs ────────────────────────────────────────────── */
.stTextInput input, .stTextArea textarea, .stSelectbox select {
    background-color: var(--navy) !important;
    border: 1px solid var(--navy-border) !important;
    color: var(--text) !important;
    font-family: var(--sans) !important;
    border-radius: 4px !important;
}
.stTextInput input:focus, .stTextArea textarea:focus {
    border-color: var(--amber) !important;
    box-shadow: 0 0 0 2px rgba(245,158,11,0.15) !important;
}

/* ── Progress bar ───────────────────────────────────────────── */
.stProgress > div > div > div {
    background-color: var(--amber) !important;
}

/* ── Alerts ─────────────────────────────────────────────────── */
.stSuccess { background: rgba(16,185,129,0.1) !important; border-color: var(--green) !important; }
.stError   { background: rgba(239,68,68,0.1) !important; border-color: var(--red) !important; }
.stWarning { background: rgba(245,158,11,0.1) !important; border-color: var(--amber) !important; }
.stInfo    { background: rgba(59,130,246,0.1) !important; }

/* ── Checkbox ───────────────────────────────────────────────── */
.stCheckbox label { color: var(--text-dim) !important; font-size: 12px !important; }

/* ── Tabs ───────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
    background: transparent !important;
    border-bottom: 1px solid var(--navy-border) !important;
    gap: 0 !important;
}
.stTabs [data-baseweb="tab"] {
    font-family: var(--mono) !important;
    font-size: 11px !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
    color: var(--text-dim) !important;
    padding: 10px 20px !important;
    border-bottom: 2px solid transparent !important;
}
.stTabs [aria-selected="true"] {
    color: var(--amber) !important;
    border-bottom-color: var(--amber) !important;
    background: transparent !important;
}

/* ── Metrics ────────────────────────────────────────────────── */
[data-testid="metric-container"] {
    background: var(--navy-card);
    border: 1px solid var(--navy-border);
    border-radius: 6px;
    padding: 12px 16px;
}
[data-testid="metric-container"] label {
    font-family: var(--mono) !important;
    font-size: 10px !important;
    text-transform: uppercase !important;
    letter-spacing: 0.1em !important;
    color: var(--text-dim) !important;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-family: var(--mono) !important;
    font-size: 28px !important;
    color: var(--text) !important;
}

/* ── Spinner ────────────────────────────────────────────────── */
.stSpinner > div { border-color: var(--amber) transparent transparent transparent !important; }

/* ── Radio ──────────────────────────────────────────────────── */
.stRadio label { color: var(--text-dim) !important; font-size: 12px !important; }
</style>
"""


# ══════════════════════════════════════════════════════════════════════════════
# DATA MODELS
# ══════════════════════════════════════════════════════════════════════════════

FIELD_MAP_COR = {
    "DOCUMENT_NUMBER": (0,   12),
    "STATUS":          (12,  22),
    "ENTITY_NAME":     (32,  132),
    "PRINCIPAL_CITY":  (232, 282),
    "PRINCIPAL_STATE": (282, 284),
    "OFFICER_NAME":    (718, 818),
    "OFFICER_TITLE":   (818, 828),
    "LAST_FILING_YEAR":(828, 832),
    "COR_EMAIL_ADDR":  (832, 932),
}

FIELD_MAP_LLC = {
    "DOCUMENT_NUMBER": (0,   12),
    "STATUS":          (12,  22),
    "ENTITY_NAME":     (32,  132),
    "PRINCIPAL_CITY":  (232, 282),
    "PRINCIPAL_STATE": (282, 284),
    "MANAGER_NAME":    (718, 818),
    "MANAGER_TITLE":   (818, 828),
    "LAST_FILING_YEAR":(828, 832),
    "COR_EMAIL_ADDR":  (832, 932),
}

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
COMPLIANCE_YEAR = 2026


# ══════════════════════════════════════════════════════════════════════════════
# SFTP + PARSE LOGIC (runs in background thread)
# ══════════════════════════════════════════════════════════════════════════════

def _extract(line: str, field_map: dict, key: str) -> str:
    if key not in field_map:
        return ""
    s, e = field_map[key]
    return line[s:min(e, len(line))].strip() if len(line) > s else ""


def _get_email(line: str, field_map: dict) -> str:
    v = _extract(line, field_map, "COR_EMAIL_ADDR")
    if v and "@" in v:
        return v.lower()
    m = EMAIL_RE.search(line)
    return m.group(0).lower() if m else ""


def _parse_file(path: Path, entity_type: str, log_q: queue.Queue) -> list[dict]:
    fmap = FIELD_MAP_LLC if entity_type == "llc" else FIELD_MAP_COR
    contact_key = "MANAGER_NAME" if entity_type == "llc" else "OFFICER_NAME"
    results = []
    total = skipped = leads = 0

    try:
        with open(path, "r", encoding="latin-1", errors="replace") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line.strip() or line.startswith("#"):
                    continue
                total += 1
                status = _extract(line, fmap, "STATUS").upper()
                yr_str = _extract(line, fmap, "LAST_FILING_YEAR")
                yr = int(yr_str) if yr_str.isdigit() else None
                email = _get_email(line, fmap)
                is_active = status.startswith("A")
                not_filed = yr is None or yr < COMPLIANCE_YEAR
                is_lead = is_active and not_filed and bool(email)

                if not is_lead:
                    skipped += 1
                    continue
                leads += 1
                results.append({
                    "doc_number":    _extract(line, fmap, "DOCUMENT_NUMBER"),
                    "entity_name":   _extract(line, fmap, "ENTITY_NAME").title(),
                    "status":        status,
                    "last_filed":    yr or "—",
                    "email":         email,
                    "contact_name":  _extract(line, fmap, contact_key).title() or "—",
                    "city":          _extract(line, fmap, "PRINCIPAL_CITY").title() or "—",
                    "entity_type":   entity_type.upper(),
                    "lead_status":   "Pending",
                    "selected":      False,
                })

        log_q.put(("ok",   f"[{entity_type.upper()}] {total:,} records → {leads:,} leads found"))
    except FileNotFoundError:
        log_q.put(("err", f"[{entity_type.upper()}] File not found: {path}"))
    except Exception as ex:
        log_q.put(("err", f"[{entity_type.upper()}] Parse error: {ex}"))

    return results


def run_sftp_scan(creds: dict, log_q: queue.Queue, result_q: queue.Queue) -> None:
    """
    Runs in a daemon thread. Posts log messages to log_q,
    then puts final DataFrame into result_q.
    """
    try:
        import paramiko
    except ImportError:
        log_q.put(("err", "paramiko not installed — pip install paramiko"))
        result_q.put(None)
        return

    host  = creds["host"]
    port  = int(creds.get("port", 22))
    user  = creds["user"]
    pwd   = creds["password"]
    staging = Path("/tmp/data/raw")
    
    log_q.put(("info", f"Connecting to {host}:{port} as {user}…"))

    transport = None
    sftp = None
    all_records = []

    try:
        # ── Connect ──────────────────────────────────────────────
        for attempt in range(1, 4):
            try:
                transport = paramiko.Transport((host, port))
                transport.connect(username=user, password=pwd)
                sftp = paramiko.SFTPClient.from_transport(transport)
                log_q.put(("ok", f"Connected (attempt {attempt})"))
                break
            except Exception as ex:
                log_q.put(("warn", f"Attempt {attempt} failed: {ex}"))
                if attempt == 3:
                    raise
                time.sleep(4)

        # ── Download each entity type ─────────────────────────────
        dirs = {
            "corp": "/public/cor/daily/",
            "llc":  "/public/llc/daily/",
        }

        for etype, remote_dir in dirs.items():
            log_q.put(("info", f"Listing {remote_dir}…"))
            try:
                entries = sftp.listdir_attr(remote_dir)
                files = [
                    e for e in entries
                    if not stat.S_ISDIR(e.st_mode) and e.filename.endswith(".txt")
                ]
                if not files:
                    log_q.put(("warn", f"No .txt files in {remote_dir}"))
                    continue

                latest = max(files, key=lambda e: e.st_mtime)
                remote_path = f"{remote_dir.rstrip('/')}/{latest.filename}"
                local_dir = staging / etype
                local_dir.mkdir(parents=True, exist_ok=True)
                local_path = local_dir / latest.filename

                log_q.put(("info", f"Downloading {latest.filename} ({latest.st_size:,} bytes)…"))
                sftp.get(remote_path, str(local_path))
                log_q.put(("ok", f"Saved → {local_path}"))

                records = _parse_file(local_path, etype, log_q)
                all_records.extend(records)

            except Exception as ex:
                log_q.put(("err", f"Error on {etype}: {ex}"))

    except Exception as ex:
        log_q.put(("err", f"SFTP fatal: {ex}"))
    finally:
        if sftp:      sftp.close()
        if transport: transport.close()
        log_q.put(("ok", f"Scan complete. {len(all_records):,} total leads."))

    if all_records:
        df = pd.DataFrame(all_records)
        result_q.put(df)
    else:
        result_q.put(None)


def generate_mock_leads(n: int = 40) -> pd.DataFrame:
    """Demo data for UI development / dry-run mode."""
    import random
    companies = [
        "Sunshine State Consulting", "Gulf Coast Ventures", "Palmetto Partners",
        "Miami Logistics Group", "Tampa Bay Holdings", "Everglades Capital",
        "Biscayne Tech LLC", "Keys Hospitality Group", "Citrus Solutions Inc",
        "Coral Gables Management", "Orlando Digital Services", "Sawgrass Enterprises",
        "Flagler Property Group", "Brickell Investment LLC", "Seminole Trading Co",
        "Okeechobee Resources", "Daytona Innovations", "Pensacola Marine Supply",
        "Treasure Coast Realty", "Space Coast Technologies",
    ]
    first = ["James","Maria","Robert","Linda","Michael","Patricia","David","Jennifer","Carlos","Susan"]
    last  = ["Rodriguez","Johnson","Williams","Garcia","Martinez","Anderson","Taylor","Thomas","Lee","White"]
    cities = ["Miami","Tampa","Orlando","Jacksonville","Fort Lauderdale","Naples","Sarasota","Gainesville"]
    statuses = ["Pending"] * 7 + ["Contacted"] * 2 + ["Paid"] * 1

    rows = []
    for i in range(n):
        name = random.choice(companies) + (" LLC" if i % 2 else " Inc")
        contact = f"{random.choice(first)} {random.choice(last)}"
        rows.append({
            "selected":      False,
            "doc_number":    f"L{random.randint(10000000,99999999):08d}",
            "entity_name":   name,
            "entity_type":   random.choice(["LLC","CORP"]),
            "contact_name":  contact,
            "email":         f"{contact.lower().replace(' ','.')}{random.randint(1,99)}@example.com",
            "city":          random.choice(cities),
            "last_filed":    random.choice([2024, 2023, 2022, "—"]),
            "status":        "ACTIVE",
            "lead_status":   random.choice(statuses),
        })
    return pd.DataFrame(rows)


# ══════════════════════════════════════════════════════════════════════════════
# EMAIL ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def build_subject(row: dict) -> str:
    return f"Courtesy Notice: May 1 Annual Report Deadline for {row['entity_name']}"


def build_body(row: dict, template: str) -> str:
    contact = row.get("contact_name", "").strip()
    greeting = f"Dear {contact}," if contact and contact != "—" else f"Dear {row['entity_name']} Team,"
    last_filed = str(row.get("last_filed", "Not on file"))
    etype = "limited liability company (LLC)" if "LLC" in str(row.get("entity_type","")) else "corporation"

    body = template.format(
        greeting=greeting,
        entity_name=row["entity_name"],
        entity_type=etype,
        city=row.get("city", "Florida"),
        last_filed=last_filed,
        doc_number=row.get("doc_number", "—"),
        email=row.get("email", ""),
    )
    return body


DEFAULT_TEMPLATE = """{greeting}

We're reaching out with a courtesy reminder on behalf of our compliance
monitoring service for Florida businesses.

Our records indicate that {entity_name}, your active {entity_type}
registered in {city}, FL, has not yet submitted its 2026 Annual Report
with the Florida Division of Corporations.

  ┌─────────────────────────────────────────────┐
  │  Filing Deadline:     May 1, 2026           │
  │  Late Fee (after):    $400.00               │
  │  Last Filing on File: {last_filed:<22}    │
  └─────────────────────────────────────────────┘

You can file directly on the official Sunbiz portal — it takes
less than 5 minutes:

  → https://dos.fl.gov/sunbiz/
  → Your document number: {doc_number}

Steps:
  1. Visit sunbiz.org and click "Annual Report"
  2. Enter your document number above
  3. Confirm officer/agent info and pay the filing fee

────────────────────────────────────────────────────────────────
We are an independent compliance reminder service — not affiliated
with the Florida Division of Corporations or any state agency.
To unsubscribe, reply with "UNSUBSCRIBE."

Florida Filing Reminders | support@yourcompany.com
Sent to {email} based on public records at sunbiz.org.
────────────────────────────────────────────────────────────────"""


def send_email_api(row: dict, cfg: dict, subject: str, body: str) -> tuple[bool, str]:
    """Live email send via SendGrid or Resend."""
    try:
        import httpx
    except ImportError:
        return False, "httpx not installed"

    provider = cfg.get("provider", "sendgrid")
    dry_run = cfg.get("dry_run", True)

    if dry_run:
        return True, f"[DRY RUN] Would send to {row['email']}"

    headers = {"Content-Type": "application/json"}
    if provider == "resend":
        headers["Authorization"] = f"Bearer {cfg['resend_key']}"
        payload = {
            "from": f"{cfg['from_name']} <{cfg['from_email']}>",
            "to": [row["email"]],
            "subject": subject,
            "text": body,
        }
        url = "https://api.resend.com/emails"
    else:
        headers["Authorization"] = f"Bearer {cfg['sendgrid_key']}"
        payload = {
            "personalizations": [{"to": [{"email": row["email"]}]}],
            "from": {"email": cfg["from_email"], "name": cfg["from_name"]},
            "subject": subject,
            "content": [{"type": "text/plain", "value": body}],
        }
        url = "https://api.sendgrid.com/v3/mail/send"

    try:
        import httpx
        r = httpx.post(url, headers=headers, json=payload, timeout=10)
        if r.status_code in (200, 201, 202):
            return True, f"Sent → {row['email']}"
        return False, f"HTTP {r.status_code}: {r.text[:120]}"
    except Exception as ex:
        return False, str(ex)


# ══════════════════════════════════════════════════════════════════════════════
# SESSION STATE BOOTSTRAP
# ══════════════════════════════════════════════════════════════════════════════

def init_state():
    defaults = {
        "leads_df":       None,
        "scan_running":   False,
        "scan_logs":      [],
        "selected_idx":   0,
        "email_template": DEFAULT_TEMPLATE,
        "sent_count":     0,
        "scan_log_q":     queue.Queue(),
        "scan_result_q":  queue.Queue(),
        "demo_mode":      True,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_state()


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════

def render_sidebar() -> tuple[dict, dict]:
    with st.sidebar:
        st.markdown("""
        <div style="padding:16px 0 20px 0; border-bottom:1px solid #1e3a5f; margin-bottom:20px;">
            <div style="font-family:'IBM Plex Mono',monospace; font-size:11px; color:#f59e0b; letter-spacing:0.12em; text-transform:uppercase; font-weight:600;">
                ⬡ Control Center
            </div>
            <div style="font-size:10px; color:#475569; margin-top:3px; font-family:'IBM Plex Mono',monospace;">
                Sunbiz Compliance v1.0
            </div>
        </div>
        """, unsafe_allow_html=True)

        # ── Mode Toggle ──────────────────────────────────────────
        st.markdown('<div style="font-size:10px;color:#64748b;text-transform:uppercase;letter-spacing:0.1em;font-weight:600;margin-bottom:6px;">Mode</div>', unsafe_allow_html=True)
        demo = st.toggle("Demo Mode (mock data)", value=st.session_state.demo_mode)
        st.session_state.demo_mode = demo
        st.markdown('<div style="height:16px"></div>', unsafe_allow_html=True)

        # ── SFTP Credentials ─────────────────────────────────────
        with st.expander("🔌 SFTP Credentials", expanded=not demo):
            sftp_host = st.text_input("Host", value="sftp.floridados.gov", disabled=demo)
            sftp_port = st.text_input("Port", value="22", disabled=demo)
            sftp_user = st.text_input("Username", value="Public", disabled=demo)
            sftp_pass = st.text_input("Password", value="PubAccess1845!" if not demo else "••••••••••••", type="password", disabled=demo)
            staging   = st.text_input("Local Staging Dir", value="./data/raw", disabled=demo)

        sftp_creds = {
            "host": sftp_host, "port": sftp_port,
            "user": sftp_user, "password": sftp_pass,
            "staging": staging,
        }

        # ── Email Config ──────────────────────────────────────────
        with st.expander("✉️ Email Settings", expanded=False):
            provider  = st.selectbox("ESP Provider", ["sendgrid", "resend"], index=0)
            sg_key    = st.text_input("SendGrid API Key", type="password", placeholder="SG.xxxxxxxxx")
            rs_key    = st.text_input("Resend API Key",   type="password", placeholder="re_xxxxxxxxx")
            from_em   = st.text_input("From Email", value="alerts@yourcompany.com")
            from_nm   = st.text_input("From Name",  value="Florida Filing Reminders")
            dry_run   = st.checkbox("Dry Run (log only, don't send)", value=True)

        email_cfg = {
            "provider": provider,
            "sendgrid_key": sg_key,
            "resend_key": rs_key,
            "from_email": from_em,
            "from_name": from_nm,
            "dry_run": dry_run,
        }

        # ── Info Box ──────────────────────────────────────────────
        st.markdown("""
        <div style="background:#0a1628;border:1px solid #1e3a5f;border-radius:6px;padding:12px;margin-top:16px;">
            <div style="font-family:'IBM Plex Mono',monospace;font-size:9px;color:#64748b;text-transform:uppercase;letter-spacing:0.1em;margin-bottom:8px;">Deadline</div>
            <div style="font-family:'IBM Plex Mono',monospace;font-size:13px;color:#f59e0b;font-weight:600;">May 1, 2026</div>
            <div style="font-size:11px;color:#475569;margin-top:4px;">Late fee: $400 per entity</div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown('<div style="height:12px"></div>', unsafe_allow_html=True)
        st.markdown('<div style="font-size:10px;color:#334155;text-align:center;">Data source: Florida DOS Sunbiz</div>', unsafe_allow_html=True)

    return sftp_creds, email_cfg


# ══════════════════════════════════════════════════════════════════════════════
# MAIN DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════

def render_header():
    st.markdown("""
    <div class="dash-header">
        <div>
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px;">
                <span class="dash-title">🏛 Sunbiz Compliance Control Center</span>
                <span class="dash-header-badge">LIVE</span>
            </div>
            <div class="dash-subtitle">Florida Annual Report · 2026 Filing Cycle · Powered by Sunbiz SFTP Feed</div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_kpis(df: pd.DataFrame | None):
    if df is None:
        total = pending = contacted = paid = 0
    else:
        total     = len(df)
        pending   = len(df[df["lead_status"] == "Pending"])
        contacted = len(df[df["lead_status"] == "Contacted"])
        paid      = len(df[df["lead_status"] == "Paid"])

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Total Leads", f"{total:,}", help="All delinquent businesses found")
    with c2:
        st.metric("Pending Outreach", f"{pending:,}", help="Not yet contacted")
    with c3:
        st.metric("Contacted", f"{contacted:,}", help="Emails sent")
    with c4:
        st.metric("Filed / Paid", f"{paid:,}", help="Confirmed compliant")


def render_scan_controls(sftp_creds: dict):
    col_btn, col_status = st.columns([2, 5])

    with col_btn:
        mode_label = "▶ Run Demo Scan" if st.session_state.demo_mode else "▶ Run Daily Scan"
        if st.button(mode_label, use_container_width=True, disabled=st.session_state.scan_running):
            if st.session_state.demo_mode:
                st.session_state.scan_logs.append(("info", "Demo mode: generating mock leads…"))
                df = generate_mock_leads(40)
                st.session_state.leads_df = df
                st.session_state.scan_logs.append(("ok", f"Demo scan complete. {len(df)} leads loaded."))
                st.rerun()
            else:
                st.session_state.scan_running = True
                st.session_state.scan_logs = [("info", "Scan initiated…")]
                st.session_state.scan_log_q  = queue.Queue()
                st.session_state.scan_result_q = queue.Queue()

                t = threading.Thread(
                    target=run_sftp_scan,
                    args=(sftp_creds, st.session_state.scan_log_q, st.session_state.scan_result_q),
                    daemon=True,
                )
                t.start()
                st.rerun()

    with col_status:
        if st.session_state.scan_running:
            # Drain log queue
            try:
                while True:
                    lvl, msg = st.session_state.scan_log_q.get_nowait()
                    st.session_state.scan_logs.append((lvl, msg))
            except queue.Empty:
                pass

            # Check for result
            try:
                result = st.session_state.scan_result_q.get_nowait()
                st.session_state.scan_running = False
                if result is not None:
                    st.session_state.leads_df = result
                else:
                    st.session_state.scan_logs.append(("warn", "Scan completed with no leads."))
                st.rerun()
            except queue.Empty:
                st.info("⟳ Scanning… (auto-refreshes)")
                time.sleep(1.5)
                st.rerun()


def render_log_console():
    st.markdown('<div class="section-title" style="margin-bottom:8px;">System Log</div>', unsafe_allow_html=True)
    logs = st.session_state.scan_logs[-60:]  # Last 60 entries
    lines = []
    for lvl, msg in logs:
        ts = datetime.now().strftime("%H:%M:%S")
        cls = {"ok": "log-ok", "err": "log-err", "warn": "log-warn"}.get(lvl, "log-info")
        prefix = {"ok": "✓", "err": "✗", "warn": "△"}.get(lvl, "›")
        lines.append(f'<span class="{cls}">[{ts}] {prefix} {msg}</span>')

    log_html = "<br>".join(lines) if lines else '<span class="log-info">No activity yet. Run a scan to begin.</span>'
    st.markdown(f'<div class="log-console">{log_html}</div>', unsafe_allow_html=True)


def render_lead_table(df: pd.DataFrame) -> int | None:
    """Renders the lead table. Returns the index of the selected row."""

    # ── Filters ─────────────────────────────────────────────────
    fc1, fc2, fc3 = st.columns([3, 2, 2])
    with fc1:
        search = st.text_input("🔍 Search", placeholder="Business name, email, city…", label_visibility="collapsed")
    with fc2:
        status_filter = st.selectbox("Status", ["All", "Pending", "Contacted", "Paid"], label_visibility="collapsed")
    with fc3:
        type_filter = st.selectbox("Type", ["All", "LLC", "CORP"], label_visibility="collapsed")

    filtered = df.copy()
    if search:
        mask = (
            filtered["entity_name"].str.contains(search, case=False, na=False) |
            filtered["email"].str.contains(search, case=False, na=False) |
            filtered["city"].str.contains(search, case=False, na=False) |
            filtered["doc_number"].str.contains(search, case=False, na=False)
        )
        filtered = filtered[mask]
    if status_filter != "All":
        filtered = filtered[filtered["lead_status"] == status_filter]
    if type_filter != "All":
        filtered = filtered[filtered["entity_type"] == type_filter]

    st.markdown(f'<div style="font-size:11px;color:#475569;margin:6px 0 10px 0;font-family:IBM Plex Mono,monospace;">{len(filtered):,} results</div>', unsafe_allow_html=True)

    if filtered.empty:
        st.markdown('<div style="padding:40px;text-align:center;color:#334155;font-family:IBM Plex Mono,monospace;font-size:12px;">No leads match the current filters.</div>', unsafe_allow_html=True)
        return None

    # ── Display columns ──────────────────────────────────────────
    display_cols = ["entity_name", "entity_type", "contact_name", "email", "city", "last_filed", "lead_status"]
    display_df = filtered[display_cols].copy()
    display_df.columns = ["Business Name", "Type", "Contact", "Email", "City", "Last Filed", "Status"]

    # Streamlit dataframe with row selection
    event = st.dataframe(
        display_df,
        use_container_width=True,
        height=340,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "Status": st.column_config.SelectboxColumn(
                options=["Pending", "Contacted", "Paid", "Skipped"],
                width="small",
            ),
            "Last Filed": st.column_config.TextColumn(width="small"),
            "Type": st.column_config.TextColumn(width="small"),
        },
    )

    # Return actual index in the *original* df for the selected row
    selected_rows = event.selection.rows if hasattr(event, "selection") else []
    if selected_rows:
        local_idx = selected_rows[0]
        if local_idx < len(filtered):
            actual_idx = filtered.index[local_idx]
            return actual_idx
    return filtered.index[0]  # default: first row


def render_email_previewer(df: pd.DataFrame, idx: int, email_cfg: dict):
    row = df.loc[idx].to_dict()
    subject = build_subject(row)
    body    = build_body(row, st.session_state.email_template)

    # ── Header ───────────────────────────────────────────────────
    st.markdown(f"""
    <div class="email-preview-wrap">
        <div class="email-preview-header">
            <div class="email-preview-field">TO: <span>{row['email']}</span></div>
            <div class="email-preview-field">SUBJECT: <span>{subject}</span></div>
            <div class="email-preview-field" style="margin-top:4px;">
                <span style="background:rgba(245,158,11,0.12);color:#f59e0b;font-family:'IBM Plex Mono',monospace;font-size:10px;padding:2px 7px;border-radius:2px;border:1px solid rgba(245,158,11,0.25);">
                    {row.get('lead_status','Pending').upper()}
                </span>
                &nbsp;
                <span style="font-family:'IBM Plex Mono',monospace;font-size:10px;color:#475569;">
                    {row['entity_type']} · Last filed: {row['last_filed']}
                </span>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Body textarea (editable) ─────────────────────────────────
    edited_body = st.text_area(
        "Email Body (editable)",
        value=body,
        height=320,
        label_visibility="collapsed",
    )

    # ── Actions ──────────────────────────────────────────────────
    ac1, ac2, ac3, ac4 = st.columns([2, 2, 2, 3])
    with ac1:
        if st.button("📤 Send This Email", use_container_width=True):
            ok, msg = send_email_api(row, email_cfg, subject, edited_body)
            if ok:
                st.success(msg)
                df.at[idx, "lead_status"] = "Contacted"
                st.session_state.leads_df = df
                st.session_state.sent_count += 1
                st.rerun()
            else:
                st.error(msg)

    with ac2:
        if st.button("✓ Mark Contacted", use_container_width=True):
            df.at[idx, "lead_status"] = "Contacted"
            st.session_state.leads_df = df
            st.rerun()

    with ac3:
        if st.button("💰 Mark Paid", use_container_width=True):
            df.at[idx, "lead_status"] = "Paid"
            st.session_state.leads_df = df
            st.rerun()

    with ac4:
        st.markdown(f'<div style="font-family:IBM Plex Mono,monospace;font-size:10px;color:#475569;padding:10px 0;">Total sent this session: <b style="color:#f59e0b">{st.session_state.sent_count}</b></div>', unsafe_allow_html=True)


def render_bulk_send(df: pd.DataFrame, email_cfg: dict):
    pending = df[df["lead_status"] == "Pending"]
    st.markdown(f'<div style="font-size:12px;color:#64748b;margin-bottom:12px;">{len(pending):,} pending leads will receive the current template.</div>', unsafe_allow_html=True)

    col1, col2 = st.columns([2, 5])
    with col1:
        if st.button(f"📤 Send to All {len(pending):,} Pending", use_container_width=True):
            progress = st.progress(0)
            status_box = st.empty()
            sent = failed = 0
            for i, (idx, row) in enumerate(pending.iterrows()):
                subject = build_subject(row.to_dict())
                body    = build_body(row.to_dict(), st.session_state.email_template)
                ok, msg = send_email_api(row.to_dict(), email_cfg, subject, body)
                if ok:
                    sent += 1
                    df.at[idx, "lead_status"] = "Contacted"
                else:
                    failed += 1
                progress.progress((i + 1) / len(pending))
                status_box.markdown(f'<span style="font-family:IBM Plex Mono,monospace;font-size:11px;color:#64748b;">Sending {i+1}/{len(pending)} — ✓ {sent} sent, ✗ {failed} failed</span>', unsafe_allow_html=True)
                time.sleep(0.05)  # rate limiting simulation

            st.session_state.leads_df = df
            st.session_state.sent_count += sent
            st.success(f"Batch complete: {sent:,} sent, {failed:,} failed.")
            st.rerun()


def render_template_editor():
    st.markdown('<div style="font-size:11px;color:#64748b;margin-bottom:8px;">Edit the template below. Use <code style="background:#0f2040;padding:1px 5px;border-radius:3px;color:#f59e0b;">{greeting}</code>, <code style="background:#0f2040;padding:1px 5px;border-radius:3px;color:#f59e0b;">{entity_name}</code>, <code style="background:#0f2040;padding:1px 5px;border-radius:3px;color:#f59e0b;">{doc_number}</code>, <code style="background:#0f2040;padding:1px 5px;border-radius:3px;color:#f59e0b;">{last_filed}</code>, <code style="background:#0f2040;padding:1px 5px;border-radius:3px;color:#f59e0b;">{city}</code>, <code style="background:#0f2040;padding:1px 5px;border-radius:3px;color:#f59e0b;">{email}</code>.</div>', unsafe_allow_html=True)
    new_template = st.text_area("Template", value=st.session_state.email_template, height=400, label_visibility="collapsed")
    c1, c2 = st.columns([2, 5])
    with c1:
        if st.button("💾 Save Template"):
            st.session_state.email_template = new_template
            st.success("Template saved.")
    with c2:
        if st.button("↺ Reset to Default"):
            st.session_state.email_template = DEFAULT_TEMPLATE
            st.rerun()


def render_export(df: pd.DataFrame):
    st.markdown('<div style="font-size:12px;color:#64748b;margin-bottom:12px;">Export the current lead list as CSV or JSONL for external processing.</div>', unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        buf = StringIO()
        df.drop(columns=["selected"], errors="ignore").to_csv(buf, index=False)
        st.download_button("⬇ Download CSV", data=buf.getvalue(), file_name="sunbiz_leads.csv", mime="text/csv", use_container_width=True)
    with c2:
        jsonl = "\n".join(df.drop(columns=["selected"], errors="ignore").to_dict(orient="records").__iter__().__class__.__name__)
        jsonl = "\n".join(json.dumps(r) for r in df.drop(columns=["selected"], errors="ignore").to_dict(orient="records"))
        st.download_button("⬇ Download JSONL", data=jsonl, file_name="sunbiz_leads.jsonl", mime="application/jsonl", use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# APP ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main():
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

    sftp_creds, email_cfg = render_sidebar()
    render_header()

    df = st.session_state.leads_df
    render_kpis(df)

    # ── Scan Controls ────────────────────────────────────────────
    st.markdown('<div style="height:4px"></div>', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Acquisition</div>', unsafe_allow_html=True)
    render_scan_controls(sftp_creds)

    if st.session_state.scan_logs:
        with st.expander("System Log", expanded=st.session_state.scan_running):
            render_log_console()

    st.markdown('<div style="height:8px"></div>', unsafe_allow_html=True)

    if df is None:
        st.markdown("""
        <div style="background:#0f2040;border:1px solid #1e3a5f;border-radius:8px;padding:48px;text-align:center;margin-top:24px;">
            <div style="font-family:'IBM Plex Mono',monospace;font-size:32px;margin-bottom:12px;color:#1e3a5f;">⬡</div>
            <div style="font-family:'IBM Plex Mono',monospace;font-size:13px;color:#334155;font-weight:600;margin-bottom:6px;">NO DATA LOADED</div>
            <div style="font-size:12px;color:#334155;">Click "Run Demo Scan" to load mock data, or configure SFTP credentials and run a live scan.</div>
        </div>
        """, unsafe_allow_html=True)
        return

    # ── Main Tabs ────────────────────────────────────────────────
    tab_leads, tab_compose, tab_bulk, tab_template, tab_export = st.tabs([
        "Lead Table", "Email Previewer", "Bulk Send", "Template Editor", "Export"
    ])

    with tab_leads:
        st.markdown('<div style="height:8px"></div>', unsafe_allow_html=True)
        selected_idx = render_lead_table(df)
        if selected_idx is not None:
            st.session_state.selected_idx = selected_idx

    with tab_compose:
        st.markdown('<div style="height:8px"></div>', unsafe_allow_html=True)
        idx = st.session_state.selected_idx
        if idx is not None and idx in df.index:
            render_email_previewer(df, idx, email_cfg)
        else:
            st.info("Select a lead from the Lead Table tab to preview its email.")

    with tab_bulk:
        st.markdown('<div style="height:8px"></div>', unsafe_allow_html=True)
        render_bulk_send(df, email_cfg)

    with tab_template:
        st.markdown('<div style="height:8px"></div>', unsafe_allow_html=True)
        render_template_editor()

    with tab_export:
        st.markdown('<div style="height:8px"></div>', unsafe_allow_html=True)
        render_export(df)


if __name__ == "__main__":
    main()
