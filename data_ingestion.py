import pandas as pd

def read_csv(filepath: str) -> pd.DataFrame:
    """Takes in a csv as input and returns a pandas dataframe of the csv's contents"""
    try:
        file_data = pd.read_csv(filepath)
        
        print("File was successfully read!")     
   
        return file_data 
    
    except FileNotFoundError as e:
        print(f"Error Loading File at {filepath}. Reason: {e}")
        return None


