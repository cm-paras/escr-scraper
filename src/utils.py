import math
from datetime import datetime, timedelta


def get_all_dates_in_year(year, total_count):
    """Generate date ranges in the specified year based on gap days."""
    add_variable = date_gap(total_count)

    # Start with January 1st of the given year
    start_date = datetime(year, 1, 1)

    # End date is December 31st of the given year
    end_date = datetime(year, 12, 31)

    # List to store date range tuples
    date_ranges = []

    # Special case: if add_variable is -1, return entire year as one range
    if add_variable == -1:
        date_ranges.append((start_date.strftime("%d.%m.%Y"), end_date.strftime("%d.%m.%Y")))
        return date_ranges

    current_start = start_date

    while current_start <= end_date:
        # Calculate the end of current range
        range_end = current_start + timedelta(days=add_variable - 1)

        # Ensure we don't exceed the year boundary
        if range_end > end_date:
            range_end = end_date

        # Add the date range tuple (start_date, end_date)
        date_ranges.append((current_start.strftime("%d.%m.%Y"), range_end.strftime("%d.%m.%Y")))

        # Move to the next range start (day after current range end)
        current_start = range_end + timedelta(days=1)

        # Break if we've reached or exceeded the year end
        if current_start > end_date:
            break

    return date_ranges


def date_gap(total_count):
    effective_judgement_days = 365 - (2 * 4 * 12)
    avg_judgement_per_day = total_count / effective_judgement_days

    gap_days = math.floor(200 / avg_judgement_per_day)

    if gap_days < 0:
        return 1
    elif gap_days > 365:
        return -1
    else:
        return gap_days


if __name__ == "__main__":
    ranges = get_all_dates_in_year(2024, 25)
    print(ranges)
