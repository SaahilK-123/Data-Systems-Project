ETL implementation with Python and Azure cloud
This project implements a simple Star Schema data warehouse design: Extracting data source from Azure blob storage, Transforming the data into Dimension and Fact tables, and finally uploading the schema to Azure SQL database for data warehouse management.

Full Stack Application
The FastAPI backend is deployed on Render after being complied into a Docker image. The Frontend application is deployed on Streamlit Community Cloud (Python version) and Vercel (HTML version). The purpose of this application is to show different data privileges for different users: Data Manager and Employee where the manager has more data access while the employee can only access personal information.

The username and password for manager are: manager1 - managerpass
There are 3 employees base on the SQL data: ann, bob, and john with the same password 1234
Please follow the Tutorial Videos for project set up and implementation

To run the application locally, follow these steps:

Create .env file in the project directory and add the required environment variables
Create a virtual environment and install the required packages: pip install -r requirements.txt
Run the python backend: uvicorn utils.api:app
Change the const api_url variable in webapp/main.js to http://127.0.0.1:8000
Open the index.html file in your browser or run command streamlit run webapp/app.py and use the app
Required Environment varables:
ACCOUNT_STORAGE="YOUR STORAGE ACCOUNT"
USERNAME_AZURE="YOUR SQL USERNAME"
PASSWORD="YOUR SQL PASSWORD"
SERVER="YOUR AZURE SQL SERVER" * MAKE SURE TO HAVE database.windows.net
DATABASE="YOUR AZURE SQL DATABASE"
JWT_SECRET_KEY="ANY SECRET KEY FOR THE APP"
Official Azure Documentations:
Azure Blob Storage

Azure SQL database

Core Prerequisites:
Azure account with an active subscription - create an account for free

Azure Storage account - create a storage account

An Azure SQL database configured with Microsoft Entra authentication. You can create one using the Create database quickstart.

The latest version of the Azure CLI.

Visual Studio Code with the Python extension.

Python 3.8 or later. If you're using a Linux client machine, see Install the ODBC driver.
