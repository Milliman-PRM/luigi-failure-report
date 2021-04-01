"""
### CODE OWNERS: Dan Buis

### OBJECTIVE:
  Parse log files and get some high level information about the different errors and exceptions that occured.

### DEVELOPER NOTES:
  This is intended to be run locally, populating the WORKSPACES iterable with whatever space(s) you are looking at
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
error_indicators = ["exception", "error"]
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


# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def main() -> int:
    """A function to enclose the execution of business logic."""
    LOGGER.info("About to do something awesome.")
    sparkapp = SparkApp(PRM_META["pipeline_signature"])

    error_collection = []

    for workspace in WORKSPACES:
        for (dirpath, dirnames, filenames) in os.walk(workspace / "02_Logs"):
            for f in filenames:
                file_path = Path(os.path.join(dirpath, f))
                if file_path.suffix == ".txt" or file_path.suffix == ".log":
                    with open(file_path) as file:
                        for line in file:
                            if any(x in line.lower() for x in error_indicators):
                                if not any(y in line for y in red_herring_errors):
                                    error_collection += [
                                        (line, str(file_path), file_path.suffix)
                                    ]

    col_names = ["error message", "file path", "file extension"]
    error_df = sparkapp.session.createDataFrame(error_collection, col_names)

    error_df = error_df.withColumn(
        "error category",
        F.when(
            F.col("error message").contains(
                "Connection refused: no further information"
            ),
            "Connection Refused",
        )
        .when(
            F.col("error message").contains(
                "An existing connection was forcibly closed by the remote host"
            ),
            "Connection closed",
        )
        .when(F.col("error message").contains("Connection reset"), "Connection reset")
        .when(F.col("error message").contains("Failed to connect"), "Connection failed")
        .when(
            F.col("error message").contains("ConnectionResetError"),
            "Connection reset error",
        )
        .when(
            F.col("error message").contains("Exception in connection from /"),
            "Connection exception",
        )
        .when(
            (F.col("error message").contains("OutOfMemoryError"))
            | (
                F.col("error message").contains("There is not enough space on the disk")
            ),
            "Out of memory",
        )
        .when(F.col("error message").contains("TimeoutException"), "Timeout error")
        .when(
            F.col("error message").contains("Error while sending"),
            "Error while sending",
        )
        .when(
            F.col("error message").contains("Error while receiving"),
            "Error while receiving",
        )
        .when(
            (F.col("error message").contains("FetchFailed"))
            | F.col("error message").contains("fetch"),
            "Fetch failure while reading to or from disk",
        )
        .when(F.col("error message").contains("PermissionError"), "Permission error")
        .when(
            F.col("error message").contains("Python worker failed to connect back"),
            "Lost worker connection",
        )
        .when(
            F.col("error message").contains(
                "Python worker exited unexpectedly (crashed)"
            ),
            "Python worker crashed",
        )
        .when(
            F.col("error message").contains("Caused by: java.io.EOFException"),
            "End of file error",
        )
        .when(
            F.col("error message").contains("UncaughtExceptions"),
            "Generic uncaught spark exception",
        )
        .otherwise("Miscellaneous"),
    ).withColumn(
        "log type",
        F.when(F.col("file extension") == ".log", "Script log").otherwise("Spark log"),
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
