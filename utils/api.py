from fastapi import FastAPI, Depends, HTTPException, status, Response
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt, JWTError
from passlib.context import CryptContext
from datetime import datetime, timedelta
from pydantic import BaseModel
from dotenv import load_dotenv
import os, json
from fastapi.middleware.cors import CORSMiddleware

from utils.datasetup import AzureDB

load_dotenv()

# Security
SECRET_KEY = os.environ.get('JWT_SECRET_KEY')
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


app = FastAPI()

# List of allowed origins
origins = ["http://127.0.0.1:5500", "https://etl-tutorial.vercel.app"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# Password context
pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Database simulation
db_users = {
    "admin": {
        "username": "admin",
        "full_name": "Admin User",
        "hashed_password": pwd_context.hash("admin123"),
        "roles": ["manager"],
        "id": 0
    },
    
    "crypto_user": {
        "username": "crypto_user",
        "full_name": "Crypto Analyst",
        "hashed_password": pwd_context.hash("user1234"),
        "roles": ["employee"],
        "id": 1
    },
}

# SQL database access
database=AzureDB()
database.access_container("csvfiles")

class Token(BaseModel):
    access_token: str
    token_type: str

class User(BaseModel):
    username: str
    roles: list
    id: int

# Authentication functions
def authenticate_user(username: str, password: str):
    user = db_users.get(username)
    if user and pwd_context.verify(password, user['hashed_password']):
        return user
    return False

def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def add_cors_headers(response: Response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'POST, GET, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Authorization, Content-Type'
    return response

async def get_current_user(token: str = Depends(oauth2_scheme)):
    
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        user_data = db_users.get(username, None)
        if user_data is None:
            raise credentials_exception
        return User(username=user_data['username'], roles=user_data['roles'], id=user_data['id'])
    except JWTError:
        raise credentials_exception

def check_user_role(role: str):
    def role_checker(current_user: User = Depends(get_current_user)):
        if role not in current_user.roles:
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        return current_user
    return role_checker

@app.post("/token", response_model=Token)
async def login_for_access_token(response: Response, form_data: OAuth2PasswordRequestForm = Depends()):
    response = add_cors_headers(response)   
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user['username'], "roles": user['roles'], "id": user['id'], "name": user['full_name']},
        expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

# API endpoints
@app.get("/")
async def home():
    return {"message": "Cryptocurrency ETL API"}

@app.get("/data/employee")
async def crypto_summary(response: Response, current_user: User = Depends(get_current_user)):
    
    response = add_cors_headers(response) 
    id = current_user.id
    employee_overview_data = f'''
        SELECT [ISO_Stdised_Key (PK)], AVG(Open_Price) AS Avg_Open, AVG(Close_Price) AS Avg_Close, AVG(Volume_Traded) AS Avg_Volume
        FROM [dbo].[Crypto_Fact]
        GROUP BY [ISO_Stdised_Key (PK)]
    '''
    return json.dumps(database.get_sql_table(employee_overview_data))

@app.get("/data/manager")
async def market_overview(response: Response, current_user: User = Depends(check_user_role("manager"))):
    response = add_cors_headers(response) 
    id = current_user.id    
    manager_overview_data = f'''
        SELECT Month_Date, AVG(Open_Price) AS Avg_Open, AVG(Close_Price) AS Avg_Close, AVG(High_Price) AS Avg_High,
               AVG(Low_Price) AS Avg_Low, AVG(Volume_Traded) AS Avg_Volume
        FROM [dbo].[Crypto_Fact]
        GROUP BY Month_Date
        ORDER BY Month_Date
    '''
    return json.dumps(database.get_sql_table(manager_overview_data))
    
# Running the app with Uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, port=8000)