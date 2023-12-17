import csv
import json

def csv_to_json(csv_file_path, json_file_path):
    # Create an empty list to store the rows
    data = []

    # Open the CSV file
    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        # Use the CSV reader to parse the file
        csv_reader = csv.DictReader(file)

        # Add each row as a dictionary to the data list
        for row in csv_reader:
            data.append(row)

    # Open the JSON file and write the data
    with open(json_file_path, mode='w', encoding='utf-8') as file:
        # Use the json.dump() method to write the JSON data
        json.dump(data, file, indent=4)

# Example usage
csv_file = 'path/to/your/csvfile.csv'
json_file = 'path/to/your/jsonfile.json'
csv_to_json(csv_file, json_file)
