import pyodbc
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=skdatasystemsserver.database.windows.net;"
    "DATABASE=DataSys_SQLDB;"
    "UID=saahilkaryekar;"
    "PWD=9Rosegum"
)
print("Connection successful!")
conn.close()