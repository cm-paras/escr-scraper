from datetime import datetime, timedelta


def get_all_dates_in_year(year):
    """Generate all dates in the specified year."""
    # Start with January 1st of the given year
    start_date = datetime(year, 1, 1)

    # Determine if it's a leap year to get the correct number of days
    if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
        # Leap year has 366 days
        num_days = 366
    else:
        # Non-leap year has 365 days
        num_days = 365

    # Generate all dates
    all_dates = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(num_days)]

    return all_dates
