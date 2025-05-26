import os
import pyodbc
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import io
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
import urllib.parse

load_dotenv()

username = os.environ.get("USERNAME_AZURE")
password = urllib.parse.quote_plus(os.environ.get("PASSWORD"))
server = os.environ.get("SERVER")
database = os.environ.get("DATABASE")
account_storage = os.environ.get("ACCOUNT_STORAGE")

odbc_str = (
    f"mssql+pyodbc://{username}:{password}"
    f"@{server}:1433/{database}"
    f"?driver=ODBC+Driver+18+for+SQL+Server"
    f"&Encrypt=yes&TrustServerCertificate=no&Connection Timeout=30"
)

engine = create_engine(odbc_str)

# Test the connection
try:
    with engine.connect() as connection:
        print("SQLAlchemy connection successful!")
except Exception as e:
    print(f"Connection failed: {e}")

class AzureDB:
    def __init__(self, local_path="./data", account_storage=account_storage):
        self.local_path = local_path
        self.account_url = f"https://{account_storage}.blob.core.windows.net"
        self.default_credentials = DefaultAzureCredential()
        self.blob_service_client = BlobServiceClient(self.account_url, credential=self.default_credentials)
        
    def access_container(self, container_name):
        try:
            self.container_client = self.blob_service_client.create_container(container_name)
            print(f"Creating container {container_name} since it does not exist in database")
            self.container_name = container_name
        except Exception as ex:
            print(f"Accessing container {container_name}")
            self.container_client = self.blob_service_client.get_container_client(container=container_name)
            self.container_name = container_name
            
    def delete_container(self):
        print("Deleting blob container...")
        self.container_client.delete_container()
        print("Done")
        
    def upload_blob(self, blob_name, blob_data=None):
        local_file_name = blob_name
        upload_file_path = os.path.join(self.local_path, local_file_name)
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=local_file_name)
        print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)
        
        if blob_data is not None:
            blob_client.upload_blob(blob_data, overwrite=True)
        else:
            with open(file=upload_file_path, mode="rb") as data:
                blob_client.upload_blob(data, overwrite=True)
                
    def list_blobs(self):
        print("\nListing blobs...")
        blob_list = self.container_client.list_blobs()
        for blob in blob_list:
            print("\t" + blob.name)
            
    def download_blob(self, blob_name):
        download_file_path = os.path.join(self.local_path, blob_name)
        print("\nDownloading blob to \n\t" + download_file_path)
        with open(file=download_file_path, mode="wb") as download_file:
            download_file.write(self.container_client.download_blob(blob_name).readall())
            
    def delete_blob(self, container_name: str, blob_name: str):
        print("\nDeleting blob " + blob_name)
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_client.delete_blob()
        
    def access_blob_csv(self, blob_name):
        try:
            print(f"Accessing blob {blob_name}")
            df = pd.read_csv(io.StringIO(self.container_client.download_blob(blob_name).readall().decode('utf-8')))
            return df
        except Exception as ex:
            print('Exception:')
            print(ex)
            return None
            
    # def upload_dataframe_sqldatabase(self, blob_name, blob_data):
    #     print("\nUploading to Azure SQL server as table:\n\t" + blob_name)
    #     blob_data.to_sql(blob_name, engine, if_exists="replace", index=False)
    #     primary = blob_name.replace("dim", "id")
        
    #     with engine.connect() as con:
    #         trans = con.begin()
    #         try:
    #             if "fact" in blob_name.lower():
    #                 con.execute(text(f"ALTER TABLE [dbo].[{blob_name}] ALTER COLUMN {blob_name}_id BIGINT NOT NULL"))
    #                 con.execute(text(f"ALTER TABLE [dbo].[{blob_name}] ADD CONSTRAINT [PK_{blob_name}] PRIMARY KEY CLUSTERED ([{blob_name}_id] ASC);"))
    #             else:
    #                 con.execute(text(f"ALTER TABLE [dbo].[{blob_name}] ALTER COLUMN {primary} BIGINT NOT NULL"))
    #                 con.execute(text(f"ALTER TABLE [dbo].[{blob_name}] ADD CONSTRAINT [PK_{blob_name}] PRIMARY KEY CLUSTERED ([{primary}] ASC);"))
    #             trans.commit()
    #         except Exception as e:
    #             trans.rollback()
    #             print(f"Error setting primary key: {e}")
    
    def upload_dataframe_sqldatabase(self, blob_name, blob_data):
        print("\nUploading to Azure SQL server as table:\n\t" + blob_name)
        
        # Name of the table in SQL Server
        table_name = f"[dbo].[{blob_name}]"
        primary = blob_name.replace("dim", "id")
        
        with engine.connect() as con:
            trans = con.begin()
            try:
                
                blob_data.to_sql(blob_name, engine, if_exists="replace", index=False)
                
                # Set primary key constraint
                if "fact" in blob_name.lower():
                    con.execute(text(f"ALTER TABLE [dbo].[{blob_name}] ALTER COLUMN {blob_name}_id BIGINT NOT NULL"))
                    con.execute(text(f"ALTER TABLE [dbo].[{blob_name}] ADD CONSTRAINT [PK_{blob_name}] PRIMARY KEY CLUSTERED ([{blob_name}_id] ASC);"))
                else:
                    con.execute(text(f"ALTER TABLE [dbo].[{blob_name}] ALTER COLUMN {primary} BIGINT NOT NULL"))
                    con.execute(text(f"ALTER TABLE [dbo].[{blob_name}] ADD CONSTRAINT [PK_{blob_name}] PRIMARY KEY CLUSTERED ([{primary}] ASC);"))
                
                trans.commit()
                
            except Exception as e:
                trans.rollback()
                print(f"Error uploading table {blob_name}: {e}")

    def append_dataframe_sqldatabase(self, blob_name, blob_data):
        print("\nAppending to table:\n\t" + blob_name)
        blob_data.to_sql(blob_name, engine, if_exists="append", index=False)
        
    def delete_sqldatabase(self, table_name):
        with engine.connect() as con:
            trans = con.begin()
            try:
                con.execute(text(f"DROP TABLE [dbo].[{table_name}]"))
                trans.commit()
                print(f"Table {table_name} deleted successfully")
            except Exception as e:
                trans.rollback()
                print(f"Error deleting table: {e}")
                
    def get_sql_table(self, query):
        
        try:
            with engine.connect() as conn:
                result = conn.execute(text(query))
                columns = result.keys()
                data = [dict(zip(columns, row)) for row in result.fetchall()]
                return data
        
        except Exception as e:
            print(f"Error in get_sql_table: {e}")
            return []
