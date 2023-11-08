import pandas as pd
from datetime import timedelta, date

# Define the Portugal public holidays for a given year range
portugal_holidays = {
    '2023': ['2023-01-01', '2023-04-07', '2023-04-09', '2023-04-25', 
             '2023-05-01', '2023-06-08', '2023-06-10', '2023-08-15', '2023-10-05',
             '2023-11-01', '2023-12-01', '2023-12-08', '2023-12-25'],
    '2022': ['2022-01-01', '2022-04-15', '2022-04-17', '2022-04-25', 
             '2022-05-01', '2022-06-10', '2022-06-16', '2022-08-15', '2022-10-05',
             '2022-11-01', '2022-12-01', '2022-12-08', '2022-12-25']
}

# Function to check if a date is a public holiday in Portugal
def is_public_holiday(date, holiday_list):
    return date.strftime('%Y-%m-%d') in holiday_list

# Generate a date range
start_date = date(2022, 6, 1)
end_date = date(2023, 6, 30)

# Function to generate the dates
def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

# Create a list of dictionaries with date, is_weekend, and is_public_holiday
date_info = [{
    'date': single_date.strftime('%Y-%m-%d'),
    'is_weekend': single_date.weekday() > 4,
    'is_public_holiday': is_public_holiday(single_date, portugal_holidays[single_date.strftime('%Y')])
} for single_date in daterange(start_date, end_date)]

# Create a DataFrame
df = pd.DataFrame(date_info)

# Save to CSV
df.to_csv('weekend_and_holidays_portugal.csv', index=False)