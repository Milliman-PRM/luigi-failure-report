"""
### CODE OWNERS: Ben Copeland, Daniel Buis

### OBJECTIVE:
    Loop through a set of static client ID prefixes and CC list with dynamic dates

### DEVELOPER NOTES:
    Requires read access to the Luigi task history database
"""
import calendar
import datetime
import logging

import luigi_failure_report

LOGGER = logging.getLogger(__name__)

BATCH_PARAMS = [
    (["0273", "1111"], ["ben.copeland@milliman.com", "daniel.buis@milliman.com", "shea.parkes@milliman.com"]),
    # (["0273WSP"], ["ben.copeland@milliman.com", "daniel.buis@milliman.com"]),
    # (['1111'], 'pierre.cornell@milliman.com~umang.gupta@milliman.com')
]


def main() -> int:
    """Main execution of logic"""
    datetime_today = datetime.date.today()

    quarter_end = datetime.date(
        datetime_today.year, datetime_today.month, 1
    ) - datetime.timedelta(days=1)

    quarter_start_est = quarter_end - datetime.timedelta(days=100)
    quarter_start = datetime.date(
        quarter_start_est.year,
        quarter_start_est.month,
        calendar.monthlen(quarter_start_est.year, quarter_start_est.month),
    ) + datetime.timedelta(days=1)

    for param in BATCH_PARAMS:
        luigi_failure_report.main(
            user=luigi_failure_report.DEFAULT_USER,
            date_start=quarter_start,
            date_end=quarter_end,
            client_id_prefixes=param[0],
            cc_list=param[1],
        )

    return 0


if __name__ == "__main__":
    import sys

    RETURN_CODE = main()
    sys.exit(RETURN_CODE)
