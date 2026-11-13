"""DQ run email: a summary table and the rejected records of one layer's run, sent over SMTP.

Used by the Silver and Gold gates right after they log their result. Off unless
`enabled` is true, and it never raises: a failed email is logged and the run continues.

The SMTP password is NOT in this file. It is read from a Databricks secret scope:
    dbutils.secrets.get(scope, "smtp_password")
"""

import html
import smtplib
from email.message import EmailMessage
from typing import Dict, List, Optional

MAX_RECORDS = 50

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


INTERNAL_PREFIXES = ("_",)  # quarantine bookkeeping columns, not shown in the email


def _cell(value) -> str:
    return "" if value is None else html.escape(str(value))


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


def build_html(layer: str, run_id: str, run_time: str, summary: Dict, records: List[Dict],
               record_columns: List[str], rule_text: Dict[str, str]) -> str:
    """HTML for one layer's run. `records` are already limited to MAX_RECORDS by the caller."""
    failed = ", ".join(f"{rule_text.get(k, k)} ({v})" for k, v in summary["failed_rules"].items()) or "none"
    status = "All checks passed" if summary["quarantined"] == 0 else f'{summary["quarantined"]} record(s) rejected'
    parts = [
        '<div style="font-family:Arial,sans-serif;font-size:14px;color:#1f2937">',
        "<h3 style=\"margin:16px 0 6px\">Summary</h3>",
        _table(["Layer", "Entity", "Checked", "Passed", "Rejected", "What was wrong", "Run ID"],
               [[layer, summary["entity"], summary["checked"], summary["passed"], summary["quarantined"], failed, run_id]]),
    ]
    if records:
        parts += [
            f"<h3 style=\"margin:16px 0 6px\">Rejected records ({summary['quarantined']})</h3>",
            _table(record_columns, [[r.get(c) for c in record_columns] for r in records]),
        ]
        if summary["quarantined"] > len(records):
            parts.append(f'<p>Showing the first {len(records)} of {summary["quarantined"]} rejected records. '
                         f"The rest are in the {_cell(layer)} quarantine table for this run.</p>")
    parts.append("</div>")
    return "".join(parts)


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


def notify_gate(spark, dbutils, *, layer: str, entity: str, label: str, job_id: Optional[str],
                run_id: Optional[str], n_good: int, n_bad: int, failed_rules: Dict[str, int],
                bad_df=None, quarantine_fqn: Optional[str] = None,
                gate_log_fqn: Optional[str] = None, notify_team: Optional[str] = None) -> bool:
    """One email for one gate run: the summary, then the rejected records of this run (up to MAX_RECORDS)."""
    run_id = run_id or "unknown"
    run_time = __import__("datetime").datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    job_label = job_id or "unknown"
    team_key = notify_team or EMAIL["team"]
    team = TEAMS.get(team_key) or {"name": team_key, "recipients": []}
    subject = f"[{EMAIL['environment']}] {team['name']} {layer} DQ run result for run {run_id} (job {job_label})"
    summary = {"entity": entity, "checked": n_good + n_bad, "passed": n_good, "quarantined": n_bad,
               "failed_rules": failed_rules}
    records, columns = [], []  # the email carries the summary only; the records are in the quarantine table
    body = build_html(layer, run_id, run_time, summary, records, columns or ["Problem"], {})
    base = EMAIL["workspace_url"].rstrip("/") + "/explore/data/"
    result = "all checks passed." if n_bad == 0 else f"{n_bad} of {n_good + n_bad} record(s) rejected."
    intro = (f'<p style="font-size:14px"><b>{html.escape(layer)} DQ run result</b>, job {html.escape(job_label)}, '
             f'run {html.escape(run_id)}: {result}</p>')

    # 1) summary first, 2) then, when records were rejected, how to see them and a big link to the quarantine table
    # Every email carries the same detail block: how to see more detail, and a big link to the quarantine table.
    detail, detail_text = "", ""
    if quarantine_fqn:
        q_url = base + quarantine_fqn.replace(".", "/")
        try:
            q_exists = spark.catalog.tableExists(quarantine_fqn)
        except Exception:
            q_exists = True
        note = "" if q_exists else (" This table is created when the first record is rejected, so the link will "
                                    "not open until then.")
        detail = (f'<p style="font-size:15px">If you want to see more detail, open the quarantine table and filter '
                  f'<b>_dq_job_run_id = {html.escape(run_id)}</b>.{html.escape(note)}</p>'
                  f'<p style="font-size:24px;font-weight:bold;margin:10px 0 20px"><a href="{q_url}">'
                  f'Open the {html.escape(layer)} quarantine table</a></p>')
        detail_text = (f"\nIf you want to see more detail, open the quarantine table and filter "
                       f"_dq_job_run_id = {run_id}.{note}\n{q_url}\n")
    history = ""
    if gate_log_fqn:
        history = (f'<p style="font-size:14px">Summary of all runs: '
                   f'<a href="{base}{gate_log_fqn.replace(".", "/")}">dq_gate_log</a></p>')
    signoff = f'<p style="font-size:14px">Thanks and regards,<br>{html.escape(EMAIL["signature"])}</p>'

    body = intro + body + detail + history + signoff
    text = (f"{layer} DQ run result, job {job_label}, run {run_id}: {result}\n"
            f"Checked {n_good + n_bad}, passed {n_good}, rejected {n_bad}.\n{detail_text}\n"
            f"Thanks and regards,\n{EMAIL['signature']}")
    return send(dbutils.secrets, enabled=EMAIL["enabled"], scope=EMAIL["scope"], host=EMAIL["host"],
                port=EMAIL["port"], sender=EMAIL["sender"], recipients=team["recipients"],
                subject=subject, html_body=body, text_body=text)
