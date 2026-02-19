import pandas as pd
import sqlite3

class DataOrchestrator:
    def __init__(self, datasets: list):
        self.datasets = datasets
        self.master_df = None
    
    def data_joining(self):
        """Combines all dataframes into one master dataframe"""
        if not self.datasets:
            print("No datasets provided to join")
            return None
        
        # ignore_index=True gives an ID 0 -> N for each row
        self.master_df = pd.concat(self.datasets, ignore_index=True) # appends all dataframes 
        
        print(self.master_df.head(5))
    
    def convert_to_sql(self):
        """Creates a sql table that takes in the master dataframe"""
        pass