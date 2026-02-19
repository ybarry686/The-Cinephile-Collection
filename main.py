import pandas as pd
import data_ingestion
from data_cleaning import DataCleaner
from data_loading import DataOrchestrator

raw_data_configs = [
    {'name': 'netflix', 'file_path': r'.\datasets\netflix_titles.csv'},
    {'name': 'hulu', 'file_path': r'.\datasets\amazon_prime_titles.csv'},
    {'name': 'amazon', 'file_path': r'.\datasets\hulu_titles.csv'}
]

cleaned_files = []

for file in raw_data_configs:
    file_data = data_ingestion.read_csv(file['file_path'])

    data_cleaner = DataCleaner(streaming_service=file['name'], dirty_data=file_data)
    cleaned_data = data_cleaner.run_all_cleaning()

    cleaned_files.append(cleaned_data)

print("All files cleaned")

for file in cleaned_files:
    print(file)
    # print(file.columns)
    # print(file.head(5))


# data_warehouse = DataOrchestrator(datasets)
# data_warehouse.data_joining()

