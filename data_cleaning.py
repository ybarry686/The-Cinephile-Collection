"""Calls 'data_ingestion.py' to clean the data before inserting it into sql"""
import pandas as pd

class DataCleaner:
    def __init__(self, streaming_service: str, dirty_data: pd.DataFrame):
        self.streaming_service = streaming_service
        self.dirty_data = dirty_data # necessary for testing 
        self.cleaned_data = self.dirty_data.copy() # creates a deep copy 
    
    def clean_columns(self):
        """Makes all column titles lowercase, snake_case, strip whitespace, & rename certain columns"""
        self.cleaned_data = self.cleaned_data.rename(columns={
                                            'type': 'media_type',
                                            'rating': 'maturity_rating',
                                            'listed_in': 'genre'
                                        }   
                            )
        
        self.cleaned_data.columns = [column.strip().lower().replace(' ', '_') for column in self.cleaned_data.columns]
                    
    def drop_columns(self):
        """Drops show_id and date_added columns"""
        self.cleaned_data = self.cleaned_data.drop(columns=['show_id'])
        self.cleaned_data = self.cleaned_data.drop(columns=['date_added'])        
            
    def clean_all_cells(self):
        cols_to_trim = self.cleaned_data.select_dtypes(include=['object']).columns # returns all columns with data type: string 

        for col in cols_to_trim:
            self.cleaned_data[col] = self.cleaned_data[col].str.strip()
    
    def cast_types(self):
        """Converts the 'release_year' column to an int datatype"""    
        self.cleaned_data['release_year'] = pd.to_numeric(self.cleaned_data['release_year'], errors='coerce').astype('Int64')
    
    def drop_duplicates(self):
        """Removes any potential duplicate entries in the dataset"""
        self.cleaned_data = self.cleaned_data.drop_duplicates(subset=['title'], keep='first')
    
    def add_streaming_service(self):
        """Add a column that has the streaming service, allowing for queries based on the streaming platform"""
        self.cleaned_data['streaming_services'] = self.streaming_service

    def add_audience_rating(self):
        """
            Allows the users to filter based on audience score (ascending & descending)
            - Will need to create call api from imdb or rotten tomatoes to get rating data
        """
        pass
    
    def run_all_cleaning(self):
        self.clean_columns()
        self.drop_columns()
        self.clean_all_cells()
        self.cast_types()
        self.drop_duplicates()
        return self.cleaned_data
        # self.add_streaming_service()
        # print(self.dirty_data.columns, self.cleaned_data.columns)