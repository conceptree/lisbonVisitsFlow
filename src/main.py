import configparser
import pandas as pd

# Step 1: Read the configuration file
config = configparser.ConfigParser()
config.read('config.ini')  # Assuming the configuration file is named 'config.ini'

# Step 2: Read the CSV files as mentioned in the configuration file
main_csv_path = config.get('CSV_Config', 'main_table_path')
secondary_csv_path = config.get('CSV_Config', 'secondary_table_path')

# Step 3: Read the list of columns to consider from the main CSV source
main_columns_to_consider = config.get('CSV_Config', 'main_table_columns').split(',')
main_table = pd.read_csv(main_csv_path, usecols=main_columns_to_consider)

# Step 4: Read the list of columns to consider from the secondary CSV source
secondary_columns_to_consider = config.get('CSV_Config', 'secondary_table_columns').split(',')
secondary_table = pd.read_csv(secondary_csv_path, usecols=secondary_columns_to_consider)

# Step 5: Merge the two sources into one table based on a configured column
merge_column = config.get('CSV_Config', 'merge_column')
merged_table = pd.merge(main_table, secondary_table, on=merge_column)

# Step 6: Save the final result in a CSV file
output_csv_path = config.get('CSV_Config', 'output_table_path')
merged_table.to_csv(output_csv_path, index=False)
