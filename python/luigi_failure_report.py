"""
### CODE OWNERS: Ben Copeland

### OBJECTIVE:
    Run a Luigi failure report and send through email

### DEVELOPER NOTES:
    Requires read access to the Luigi task history database
"""
import datetime
import logging
import os
import pprint
import smtplib
import typing
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
import psycopg2

LOGGER = logging.getLogger(__name__)


def query_task_history(
    user: str = os.environ["username"], host: str = "10.3.200.99", port: str = "5432"
) -> typing.Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Query the task history into pandas dataframes"""
    with psycopg2.connect(database="luigi", user=user, host=host, port=port) as conn:
        task_events = pd.read_sql("select * from public.task_events", conn)
        task_parms = pd.read_sql("select * from public.task_parameters", conn)
        tasks = pd.read_sql("select * from public.tasks", conn).set_index("id")
        return task_events, task_parms, tasks


def create_master_task_history(
    task_events: pd.DataFrame,
    task_parms: pd.DataFrame,
    tasks: pd.DataFrame,
    date_start: typing.Optional[datetime.date] = None,
    date_end: typing.Optional[datetime.date] = None,
    client_id_prefixes: typing.Optional[typing.Iterable[str]] = None,
) -> pd.DataFrame:
    """Merge all available task information together and limit to applicable tasks"""
    if date_start:
        task_events = task_events.loc[lambda df: df.ts >= pd.Timestamp(date_start)]
    if date_end:
        task_events = task_events.loc[lambda df: df.ts <= pd.Timestamp(date_end)]

    task_parms_wide = task_parms.loc[lambda df: df.name == "pipeline_signature"].pivot(
        index="task_id", columns="name", values="value"
    )

    task_master = (
        task_events.join(task_parms_wide, on="task_id")
        .join(tasks.rename({"task_id": "task_name"}, axis="columns"), on="task_id")
        .assign(
            hour=lambda df: df.ts.dt.hour,
            dayofweek=lambda df: df.ts.dt.dayofweek,
            client_id=lambda df: df.pipeline_signature.str[:7],
            yearmo=lambda df: df.ts.dt.strftime("%Y-%m"),
        )
        .loc[
            lambda df: (
                (df.pipeline_signature.str.lower().str.contains("5-support_files"))
                | (df.pipeline_signature.str.lower().str.contains("5_support_files"))
            )
        ]
        .loc[lambda df: ~df.name.str.startswith("client.")]
    )
    if client_id_prefixes:
        regex_contains = "|".join(client_id_prefixes)
        task_master = task_master.loc[
            lambda df: df.client_id.str.contains(regex_contains)
        ]
    return task_master


def summarize_failure_rate(
    task_master: pd.DataFrame,
    levels: typing.Iterable[str],
    do_print: bool = True,
    n_min_runs_to_print: int = 10,
) -> pd.DataFrame:
    """Summarize failure rate at the requested summary level"""
    if "event_name" in levels:
        raise ValueError("Can not summarize event_name")

    event_counts = task_master.groupby([*levels, "event_name"]).size()
    event_counts.rename("size", inplace=True)
    event_counts_pivot = (
        event_counts.reset_index()
        .pivot_table(index=levels, columns="event_name", values="size")
        .fillna(0)
        .assign(failure_rate=lambda df: df.FAILED / df.RUNNING)
    )
    event_counts_pivot.columns.rename(None, inplace=True)
    top_failure_rates = (
        event_counts_pivot.loc[
            lambda df: df.RUNNING >= n_min_runs_to_print, ["RUNNING", "failure_rate"]
        ]
        .sort_values(["failure_rate", "RUNNING"], ascending=False)
        .head(30)
        .rename({"RUNNING": "count_runs"}, axis="columns")
    )

    if do_print:
        print(f"Highest failure rates for {levels}:\n{top_failure_rates}\n")
    return top_failure_rates


def format_email(
    map_failure_rates: typing.Mapping[str, pd.DataFrame],
    date_start: typing.Optional[datetime.date] = None,
    date_end: typing.Optional[datetime.date] = None,
    client_id_prefixes: typing.Optional[typing.Iterable[str]] = None,
) -> str:
    """Get formatted email contents"""

    run_parms_w_nulls = {
        "date_start": date_start,
        "date_end": date_end,
        "client_id_prefixes": client_id_prefixes,
    }
    run_parms = {k: str(v) for k, v in run_parms_w_nulls.items() if v is not None}
    email_subject = f"Luigi failure report"
    try:
        email_subject += f" from {run_parms['date_start']}"
    except KeyError:
        pass

    try:
        email_subject += f" thru {run_parms['date_end']}"
    except KeyError:
        pass

    try:
        email_subject += f" for {run_parms['client_id_prefixes']}"
    except KeyError:
        pass

    email_sections = [f"<h2>Luigi failure rate report for indy-luigi.milliman.com</h2>"]
    if run_parms:
        email_sections.append(
            f"<p>Parameters:\n{pprint.pformat(run_parms)}</p>".replace("\n", "<br />")
        )

    table_summaries_html = []
    for summary_level, top_failure_rates in map_failure_rates.items():
        formatted_table_html = (
            top_failure_rates.astype({"count_runs": "int64"})
            .reset_index()
            .to_html(
                formatters={"failure_rate": "{:,.2%}".format},
                justify="left",
                index=False,
            )
        )
        formatted_table_section = f'<h3>Top failure rates for "{summary_level}":</h3><br />{formatted_table_html}'
        table_summaries_html.append(formatted_table_section)

    table_summaries_combined = "<br /><br />".join(table_summaries_html)
    email_sections.append(table_summaries_combined)

    email_body = "<br />".join(email_sections)
    return email_subject, email_body


def send_email(
    email_subject: str, email_body: str, cc_list: typing.Iterable[str]
) -> None:
    """Send an email to anyone on the CC list"""

    msg = MIMEMultipart("alternative")
    from_addr = f"{os.environ['username']}@milliman.com"
    to_addr = ",".join(cc_list)
    msg["Subject"] = email_subject
    msg["From"] = from_addr
    msg["To"] = to_addr

    html_part = MIMEMultipart(_subtype="related")
    body = MIMEText(email_body, _subtype="html")
    html_part.attach(body)
    msg.attach(html_part)

    # Send the message via local SMTP server.
    smtp = smtplib.SMTP("smtp.milliman.com")
    smtp.sendmail(from_addr, to_addr, msg.as_string())
    smtp.quit()


def main(
    date_start: typing.Optional[datetime.date] = None,
    date_end: typing.Optional[datetime.date] = None,
    client_id_prefixes: typing.Optional[typing.Iterable[str]] = None,
    cc_list: typing.Optional[typing.Iterable[str]] = None,
) -> int:
    """Main execution of logic"""
    task_events, task_parms, tasks = query_task_history()

    task_master = create_master_task_history(
        task_events,
        task_parms,
        tasks,
        date_start=date_start,
        date_end=date_end,
        client_id_prefixes=client_id_prefixes,
    )

    summary_levels = (
        ["name"],
        ["host"],
        ["host", "client_id"],
        ["client_id"],
        ["pipeline_signature"],
        #        ['hour'],
        #        ['dayofweek'],
        #        ['yearmo'],
        #        ['yearmo', 'client_id'],
        #        ['yearmo', 'hour'],
        #        ['yearmo', 'name'],
    )

    map_failure_rates = {
        "_".join(levels): summarize_failure_rate(task_master, levels)
        for levels in summary_levels
    }

    if cc_list:
        email_subject, email_body = format_email(
            map_failure_rates, date_start, date_end, client_id_prefixes
        )
        send_email(email_subject, email_body, cc_list)

    return 0


if __name__ == "__main__":
    import argparse
    import sys

    ARGPARSER = argparse.ArgumentParser(
        description="Execute a Luigi task history summary"
    )
    ARGPARSER.add_argument(
        "-c", "--cc_list", help="Tilde-delimited list of email addresses", default=None
    )
    ARGPARSER.add_argument(
        "-p",
        "--prefixes",
        help="Tilde-delimited list of client code prefixes for limiting tasks",
        default=None,
    )
    ARGPARSER.add_argument(
        "-s",
        "--date_start",
        help="ISO format. Ignore tasks that are earlier than this date cutoff",
        default=None,
    )
    ARGPARSER.add_argument(
        "-e",
        "--date_end",
        help="ISO format. Ignore tasks that are later than this date cutoff",
        default=None,
    )
    ARGS = ARGPARSER.parse_args()

    if ARGS.date_start:
        DATE_START = datetime.date.fromisoformat(ARGS.date_start)
    else:
        DATE_START = None

    if ARGS.date_end:
        DATE_END = datetime.date.fromisoformat(ARGS.date_end)
    else:
        DATE_END = None

    try:
        CLIENT_PREFIXES = ARGS.prefixes.split("~")
    except AttributeError:
        CLIENT_PREFIXES = None

    try:
        CC_LIST = ARGS.cc_list.split("~")
    except AttributeError:
        CC_LIST = None

    RETURN_CODE = main(DATE_START, DATE_END, CLIENT_PREFIXES, CC_LIST)
    sys.exit(RETURN_CODE)
