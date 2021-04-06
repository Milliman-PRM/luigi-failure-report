"""
### CODE OWNERS: Dan Buis

### OBJECTIVE:
  Parse log files and get some high level information about the different errors and exceptions that occured.

### DEVELOPER NOTES:
  This is intended to be run locally, populating the WORKSPACES iterable with whatever space(s) you are looking at.
  Spark errors should be taken with a grain of salt as there are often non-fatal errors in the logs.
"""
import logging
import os
import re
from pathlib import Path

import prm.meta.project
from prm.spark.app import SparkApp
from pyspark.sql import functions as F

LOGGER = logging.getLogger(__name__)
PRM_META = prm.meta.project.parse_project_metadata()

WORKSPACES = [Path(r"S:\PHI\0273WSP\WSP_MSSP_2021\xx-dev\test_switch_dell")]
error_indicators = [
    "exception",
    "error",
    "INFO:",
    "WARNING:",
    "NOTE: Variable",
    "NOTE: Invalid",
    "NOTE: Library",
    "NOTE: MERGE Statement",
    "NOTE: A hardware",
    "NOTE: Character",
    "NOTE: Missing",
    "NOTE: Division",
    "NOTE: Mathematical",
    "NOTE: Interact",
    "NOTE: SAS set option OBS=0",
]
red_herring_errors = [
    "Not validating SparkApp cache due to prior exception",
    "failed with return code",
    "ERROR: The process",
    "During handling of the above exception, another exception occurred:",
    "saved parquet references",
    "Error_Ch",
    "Global Macro launcher_",
    "root|INFO|Exception while sending command",
]

error_message_map = {
    "Connection refused: no further information": "Connection refused",
    "connection was forcibly closed": "Connection closed",
    "Connection reset": "Connection reset",
    "Failed to connect": "Connection failed",
    "ConnectionResetError": "Connection reset error",
    "Exception in connection from /": "Connection Exception",
    "OutOfMemoryError": "Out of memory",
    "There is not enough space on the disk": "Out of memory",
    "TimeoutException": "Timeout error",
    "Error while sending": "Error while sending",
    "Error while receiving": "Error while receiving",
    "FetchFailed": "Fetch failure while reading to or from disk",
    "fetch": "Fetch failure while reading to or from disk",
    "PermissionError": "Permissions error",
    "Python worker failed to connect back": "Lost worker connection",
    "Python worker exited unexpectedly (crashed)": "Python worker crashed",
    "Caused by: java.io.EOFException": "End of file exception",
    "UncaughtExceptions": "Uncaught exception",
}

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def collect_logfiles():
    """Compiles a list of log files"""
    logfile_list = []
    for workspace in WORKSPACES:
        for (dirpath, dirnames, filenames) in os.walk(workspace / "02_Logs"):
            for f in filenames:
                file_path = Path(os.path.join(dirpath, f))
                if file_path.suffix == ".log":
                    logfile_list += [file_path]

    return logfile_list


def parse_log_file(file_path):
    """Parse the given log file and return a tupleized summary"""
    with open(file_path) as file:
        spark_log_path = None
        for line in file:
            if "Redirecting Spark logging to S" in line:
                spark_log_path = line.split(" ")[-1].replace("\n", "")
            if any(x in line.lower() for x in error_indicators):
                if not any(y in line for y in red_herring_errors):
                    return (line, str(file_path), spark_log_path)


def determine_spark_error(file_path):
    """Parse the given log file and return a tupleized summary"""
    errors = []
    with open(file_path) as file:
        for line in file:
            if any(x in line.lower() for x in error_indicators):
                if not any(y in line for y in red_herring_errors):
                    errors += [line]
    last_error = errors[-1]
    error_description = None
    for key in error_message_map.keys():
        if key in last_error.lower():
            error_description = error_message_map[key]

    if error_description is None:
        error_description = "Miscellaneous error"

    return (last_error, error_description, file_path)


def main() -> int:
    """A function to enclose the execution of business logic."""
    LOGGER.info("About to do something awesome.")
    sparkapp = SparkApp(PRM_META["pipeline_signature"])

    error_collection = []

    log_file_list = collect_logfiles()

    for log in log_file_list:
        log_tuple = parse_log_file(log)
        if log_tuple:
            error_collection += [log_tuple]

    log_df_col_names = ["script error message", "script file path", "spark logfile"]
    log_error_df = sparkapp.session.createDataFrame(error_collection, log_df_col_names)

    spark_error_collection = []
    for error in error_collection:
        if error[-1] is not None:
            spark_error_collection += [determine_spark_error(error[-1])]

    spark_df_col_names = [
        "spark error message",
        "spark error description",
        "spark logfile",
    ]
    spark_error_df = sparkapp.session.createDataFrame(
        spark_error_collection, spark_df_col_names
    )

    error_summary_df = log_error_df.join(
        spark_error_df, "spark logfile", "left_outer"
    ).select(
        F.col("script error message"),
        F.col("spark error message"),
        F.col("spark error description"),
        F.col("script file path"),
        F.col("spark logfile"),
    )

    return 0


if __name__ == "__main__":
    # pylint: disable=wrong-import-position, wrong-import-order, ungrouped-imports
    import sys
    import prm.utils.logging_ext
    import prm.spark.defaults_prm

    prm.utils.logging_ext.setup_logging_stdout_handler()
    SPARK_DEFAULTS_PRM = prm.spark.defaults_prm.get_spark_defaults(PRM_META)

    with SparkApp(PRM_META["pipeline_signature"], **SPARK_DEFAULTS_PRM):
        RETURN_CODE = main()

    sys.exit(RETURN_CODE)
