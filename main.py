import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
from utils.datasetup import *
import pandas as pd
from utils.dimension_classes import * 

class MainETL():
    def __init__(self) -> None:
        self.drop_columns = []
        self.dimension_tables = []
    
    def extract(self, csv_file="Cryptocurrency_Combined_Data_Tables.csv"):
        print(f"Step 1: Extracting data from csv file")
        self.fact_table = df
        print(f"We find {len(self.fact_table.index)} rows and {len(self.fact_table.columns)} columns in csv file: {csv_file}")
        print(f"Step 1 finished")
        
    def transform(self):
        
        self.fact_table[["Open_Price", "High_Price", "Low_Price", 
                         "Close_Price", "Change_pct"]] = self.fact_table[["Open_Price", "High_Price", "Low_Price",
                                                                                           "Close_Price", "Change_pct"]].astype(float)
        
        def convert_volume(x):
            x = str(x).replace(",", "")
            if x.endswith('B'):
                return float(x[:-1]) * 1e9
            elif x.endswith('M'):
                return float(x[:-1]) * 1e6
            else:
                return float(x)

        self.fact_table["Volume_Traded"] = self.fact_table["Volume_Traded"].apply(convert_volume)
        self.fact_table[["ISO_Stdised_Key (PK)"]] = self.fact_table[["ISO_Stdised_Key (PK)"]].astype(str)
        self.fact_table[["Month_Date"]] = self.fact_table[["Month_Date"]].astype(str)
        self.fact_table[["Crypto_Key (FK)"]] = self.fact_table[["Crypto_Key (FK)"]].astype(int)
        
        dim_crypto = DimCrypto()
        self.drop_columns += dim_crypto.columns
        self.dimension_tables.append(dim_crypto)
        
        dim_date = DimDate()
        self.drop_columns += dim_date.columns
        new_dim = dim_date.dimension_table[["Month_Date"]]
        new_dim["Month_Date"] = pd.to_datetime(new_dim["Month_Date"], format = "%b, %Y")
        new_dim["Month_Date"] = new_dim["Month_Date"].dt.strftime("%B %Y")
        new_dim = new_dim.drop_duplicates()
        new_dim[f"Date_ID"] = range(1, len(new_dim) + 1)
        dim_date.dimension_table = new_dim
        self.dimension_tables.append(dim_date)
        self.fact_table["Month_Date"] = pd.to_datetime(self.fact_table["Month_Date"], format = "%b, %Y")
        self.fact_table["Month_Date"] = self.fact_table["Month_Date"].dt.strftime("%B %Y")
        
        dim_price_var = DimPriceVariations()
        self.drop_columns += dim_price_var.columns
        dim_price_var.dimension_table["Price_Variation"] = (dim_price_var.dimension_table["High_Price"] - dim_price_var.dimension_table["Low_Price"])
        dim_price_var.dimension_table["Price_Variation_Type"] = dim_price_var.dimension_table["Price_Variation"].apply(lambda x: "DOWN" if x > 0 else "UP" if x < 0 else "UNCHANGED")
        dim_price_var.dimension_table = dim_price_var.dimension_table.drop_duplica3()
        dim_price_var.dimension_table["Price_Variation_ID"] = range(1, len(dim_price_var.dimension_table) + 1)
        self.dimension_tables.append(dim_price_var)

        dim_price_diff = DimPriceDifferential()
        self.drop_columns += dim_price_diff.columns
        dim_price_diff.dimension_table["Price_Differential"] = (dim_price_diff.dimension_table["Open_Price"] - dim_price_diff.dimension_table["Close_Price"])
        dim_price_diff.dimension_table["Price_Differential_Type"] = dim_price_diff.dimension_table["Price_Differential"].apply(lambda x: "DOWN" if x > 0 else "UP" if x < 0 else "UNCHANGED")
        dim_price_diff.dimension_table = dim_price_diff.dimension_table.drop_duplicates()
        dim_price_diff.dimension_table["Price_Differential_ID"] = range(1, len(dim_price_diff.dimension_table) + 1)
        self.dimension_tables.append(dim_price_diff)
        
        for dim in self.dimension_tables:
            self.fact_table = pd.merge(self.fact_table, dim.dimension_table, on=dim.columns, how="left")
        self.fact_table = self.fact_table = self.fact_table.drop(columns=self.drop_columns)
        
        print(f"Step 2 finished")
        
    def load(self):
        for table in self.dimension_tables:
            table.load()
        with engine.connect() as con:
            trans = con.begin()
            self.fact_table["Crypto_Fact_ID"] = range(1, len(self.fact_table) + 1)
            database.upload_dataframe_sqldatabase(f"Crypto_Fact", blob_data=self.fact_table)
            
            self.fact_table.to_csv("./data/Crypto_Fact.csv")
            
            for table in self.dimension_tables:
                con.execute(text(f'ALTER TABLE [dbo].[Crypto_Fact] WITH NOCHECK ADD CONSTRAINT [FK_{table.name}_dim] FOREIGN KEY ([{table.name}_id]) REFERENCES [dbo].[{table.name}_dim] ([{table.name}_id]) ON UPDATE CASCADE ON DELETE CASCADE;'))
            trans.commit()
            
        print(f"Step 3 finished")
        
    def mainLoop(self):
        
        self.extract()
        self.transform()
        self.load()
        
def main():
    main = MainETL()
    main.mainLoop()

if __name__ == "__main__":
    main()