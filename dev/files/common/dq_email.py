"""DQ run email: ONE email per layer per run, sent over SMTP.

Each quality gate records its result with record_gate(). When the layer has gone through all its
tables, the notebook calls send_layer_summary(), which sends one email with a row per table.
Off unless `enabled` is true, and it never raises: a failed email is logged and the run continues.

The SMTP password is NOT in this file. It is read from a Databricks secret scope:
    dbutils.secrets.get(scope, "smtp_password")
"""

import html
import smtplib
from email.message import EmailMessage
from typing import Dict, List, Optional


# Settings. Enabled only when a run should send mail. The password is never here: it is read from
# the Databricks secret scope below.
EMAIL = {
    "enabled": True,
    "environment": "dev",
    "team": "rls_testing",       # default team when a notebook does not name one
    "scope": "dq-alerts",
    "host": "de-smtp-outbound-1.mimecast.com",
    "port": 587,
    "sender": "dp-ops-reports@alterdomus.com",
    "workspace_url": "https://dbc-445a81b8-2855.cloud.databricks.com",
    "signature": "Data Platform Team",
}

# One entry per squad. A squad gets the same email template, with its own name and recipients.
# To add a squad: add an entry here and pass team="<key>" in its gate's notify settings.
TEAMS = {
    "openfga": {"name": "OpenFGA", "recipients": ["shiva.sai@alterdomus.com"]},
    "rls_testing": {"name": "RLS Testing", "recipients": ["shiva.sai@alterdomus.com"]},
}

def configure(environment: str, team_key: str) -> None:
    """Set the environment label and the default squad for this run's emails (from the bundle variables)."""
    if environment:
        EMAIL["environment"] = environment
    if team_key:
        EMAIL["team"] = team_key


class _Html(str):
    """A cell that is already HTML (a link). Everything else is escaped."""


def _cell(value) -> str:
    if value is None:
        return ""
    return str(value) if isinstance(value, _Html) else html.escape(str(value))


def _table(headers: List[str], rows: List[List], widths: Optional[List[int]] = None) -> str:
    head = "".join(f'<th style="text-align:left;padding:6px 10px;background:#eef1f5;border:1px solid #d0d7e2">{_cell(h)}</th>'
                   for h in headers)
    body = ""
    for row in rows:
        body += "<tr>" + "".join(
            f'<td style="padding:6px 10px;border:1px solid #d0d7e2;vertical-align:top">{_cell(v)}</td>' for v in row
        ) + "</tr>"
    return (f'<table style="border-collapse:collapse;font-family:Arial,sans-serif;font-size:14px;width:100%">'
            f"<tr>{head}</tr>{body}</table>")


def send(spark_secrets, *, enabled: bool, scope: str, host: str, port: int, sender: str,
         recipients: List[str], subject: str, html_body: str, text_body: str) -> bool:
    """Send one HTML email. Returns True when sent. Logs and returns False on any failure."""
    if not enabled:
        print("DQ email disabled (dq_email_enabled=false): not sent.")
        return False
    try:
        password = spark_secrets.get(scope, "smtp_password")
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = sender
        msg["To"] = ", ".join(recipients)
        msg.set_content(text_body)
        msg.add_alternative(html_body, subtype="html")
        with smtplib.SMTP(host, port, timeout=30) as s:
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.login(sender, password)
            s.send_message(msg)
        print(f"DQ email sent: {subject}")
        return True
    except Exception as e:  # never stop the data run for an email
        print(f"DQ email NOT sent ({type(e).__name__}): {str(e)[:200]}")
        return False


# Results of the gates of this run, in the order they ran. Filled by record_gate(), emptied by send_layer_summary().
RESULTS: List[Dict] = []


def record_gate(*, layer: str, entity: str, label: str, n_good: int, n_bad: int, failed_rules: Dict[str, int],
                quarantine_fqn: Optional[str] = None, gate_log_fqn: Optional[str] = None) -> None:
    """Keep one gate's result for the layer email. Sends nothing."""
    RESULTS.append({"layer": layer, "entity": entity, "label": label, "n_good": n_good, "n_bad": n_bad,
                    "failed_rules": failed_rules or {}, "quarantine_fqn": quarantine_fqn,
                    "gate_log_fqn": gate_log_fqn})


def send_layer_summary(dbutils, *, layer: str, job_id: Optional[str], run_id: Optional[str],
                       notify_team: Optional[str] = None) -> bool:
    """Send ONE email for this layer: a row per table that went through the gate, then how to see more detail.

    The gate results of the layer are used up, so a second call in the same run sends nothing. Returns True when
    an email was sent.
    """
    rows = [r for r in RESULTS if r["layer"] == layer]
    RESULTS[:] = [r for r in RESULTS if r["layer"] != layer]
    if not rows:
        print(f"DQ email: no {layer} gate ran in this run, nothing to send.")
        return False

    run_id = run_id or "unknown"
    job_label = job_id or "unknown"
    team_key = notify_team or EMAIL["team"]
    team = TEAMS.get(team_key) or {"name": team_key, "recipients": []}
    subject = f"[{EMAIL['environment']}] {team['name']} {layer} DQ run result for run {run_id} (job {job_label})"

    base = EMAIL["workspace_url"].rstrip("/") + "/explore/data/"
    total_bad = sum(r["n_bad"] for r in rows)
    with_rejects = [r for r in rows if r["n_bad"] > 0]
    tables = f"{len(rows)} table{'s' if len(rows) != 1 else ''}"
    result = (f"all checks passed for {tables}." if total_bad == 0
              else f"{total_bad} record(s) rejected, in {len(with_rejects)} of {tables}.")

    def rule_label(r, rule):
        """The rule name without the table name in front: the Table column already says which table."""
        prefix = f"{r['entity']}_"
        return rule[len(prefix):] if rule.startswith(prefix) and len(rule) > len(prefix) else rule

    def failed_text(r):
        return ", ".join(f"{rule_label(r, k)} ({v})" for k, v in r["failed_rules"].items()) or "none"

    def q_url(r):
        return base + r["quarantine_fqn"].replace(".", "/")

    def name_cell(r):
        """The table name; a link to its quarantine table when it has rejected rows."""
        if r["n_bad"] > 0 and r["quarantine_fqn"]:
            return _Html(f'<a href="{q_url(r)}">{html.escape(r["entity"])}</a>')
        return r["entity"]

    table_rows = [[name_cell(r), r["n_good"] + r["n_bad"], r["n_good"], r["n_bad"], failed_text(r)] for r in rows]

    intro = (f'<p style="font-size:14px"><b>{html.escape(layer)} DQ run result</b>, job {html.escape(job_label)}, '
             f"run {html.escape(run_id)}: {html.escape(result)}</p>")
    summary = ('<div style="font-family:Arial,sans-serif;font-size:14px;color:#1f2937">'
               '<h3 style="margin:16px 0 6px">Summary</h3>'
               + _table(["Table", "Checked", "Passed", "Rejected", "What was wrong"], table_rows)
               + "</div>")

    hint = " (click the table name above)" if with_rejects else ""
    detail = (f'<p style="font-size:14px">If you want to see more detail, open the quarantine table{hint} and filter '
              f"<b>_dq_job_run_id = {html.escape(run_id)}</b>.</p>")
    detail_text = (f"\nIf you want to see more detail, open the quarantine table and filter "
                   f"_dq_job_run_id = {run_id}.\n")
    if with_rejects:
        for r in with_rejects:
            if r["quarantine_fqn"]:
                detail_text += f"{r['entity']}: {q_url(r)}\n"
    else:
        detail += ('<p style="font-size:14px">No record was rejected in this run, so no quarantine table was '
                   "written.</p>")
        detail_text += "No record was rejected in this run, so no quarantine table was written.\n"

    gate_log_fqn = next((r["gate_log_fqn"] for r in rows if r["gate_log_fqn"]), None)
    history = (f'<p style="font-size:14px">Summary of all runs: '
               f'<a href="{base}{gate_log_fqn.replace(".", "/")}">dq_gate_log</a></p>') if gate_log_fqn else ""
    signoff = f'<p style="font-size:14px">Thanks and regards,<br>{html.escape(EMAIL["signature"])}</p>'

    body = intro + summary + detail + history + signoff
    lines = "\n".join(f"  {r['entity']}: checked {r['n_good'] + r['n_bad']}, passed {r['n_good']}, "
                      f"rejected {r['n_bad']} ({failed_text(r)})" for r in rows)
    text = (f"{layer} DQ run result, job {job_label}, run {run_id}: {result}\n{lines}\n{detail_text}\n"
            f"Thanks and regards,\n{EMAIL['signature']}")
    return send(dbutils.secrets, enabled=EMAIL["enabled"], scope=EMAIL["scope"], host=EMAIL["host"],
                port=EMAIL["port"], sender=EMAIL["sender"], recipients=team["recipients"],
                subject=subject, html_body=body, text_body=text)
