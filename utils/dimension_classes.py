from utils.datasetup import *
import pandas as pd

blob_name = "Cryptocurrency_Combined_Data_Tables.csv"
database = AzureDB()
database.access_container("csvfiles") # if fail use test-container
df = database.access_blob_csv(blob_name=blob_name)

class ModelAbstract():
    
    def __init__(self):
        self.columns = None
        self.dimension_table = None
        
    def dimension_generator(self, name: str, columns: list):
        dim = df[columns]
        dim = dim.drop_duplicates()
        
        dim[f'{name}_id'] = range(1, len(dim) + 1)
        
        self.dimension_table = dim
        self.name = name
        self.columns = columns
    
    def load(self):
        
        if self.dimension_table is not None:
            database.upload_dataframe_sqldatabase(f"{self.name}_dim", blob_data=self.dimension_table)
            
            self.dimension_table.to_csv(f"./data/{self.name}_dim.csv")
            
        else:
            print("Please create a dimension table first using dimension_generator")

class DimDate(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator("Date", ["Month_Date"])

class DimPriceVariations(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator("PriceVariations", ["High_Price", "Low_Price"])
        
class DimPriceDifferential(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator("PriceDifferential", ["Open_Price", "Close_Price"])
        
class DimCrypto(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator("Crypto", ["ISO_Stdised_Key (PK)", "Crypto_Key (FK)", "Volume_Traded"])

