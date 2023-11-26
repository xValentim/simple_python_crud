from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import boto3
import os
import asyncio
import logging
import time
from botocore.exceptions import ClientError, NoCredentialsError
from contextlib import asynccontextmanager

app = FastAPI()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
print(DB_HOST)
database_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:3306/{DB_NAME}"
if database_url is None:
    raise Exception("DATABASE_URL not set in environment variables")

engine = create_engine(database_url)
Base = declarative_base()

async def get_secret():
    secret_name = "app/mysql/credentials"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    loop = asyncio.get_running_loop()

    try:
        # Unpack the dictionary into keyword arguments
        get_secret_value_response = await loop.run_in_executor(
            None,  # Uses the default executor
            lambda: client.get_secret_value(SecretId=secret_name)
        )
    except ClientError as e:
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']

    return eval(secret)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Create a CloudWatch log client
try:
    log_client = boto3.client('logs', region_name="us-east-1")
except NoCredentialsError:
    logger.error("AWS credentials not found")

LOG_GROUP = '/my-fastapi-app/logs'
LOG_STREAM = os.getenv("INSTANCE_ID")

# Function to push logs to CloudWatch
import asyncio


async def push_logs_to_cloudwatch(log_message):
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(
            None,  # Uses the default executor (which is a ThreadPoolExecutor)
            lambda: log_client.put_log_events(
                logGroupName=LOG_GROUP,
                logStreamName=LOG_STREAM,
                logEvents=[
                    {
                        'timestamp': int(round(time.time() * 1000)),
                        'message': log_message
                    },
                ],
            )
        )
    except Exception as e:
        logger.error(f"Error sending logs to CloudWatch: {e}")

class Item(Base):
    __tablename__ = 'items'
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), index=True)
    description = Column(String(255), index=True)
SessionLocal = sessionmaker(bind=engine)

Base.metadata.create_all(bind=engine)
print("Database initialized")
@app.post("/items/")
async def create_item(name: str, description: str):
    db = SessionLocal()
    new_item = Item(name=name, description=description)
    db.add(new_item)
    db.commit()
    db.refresh(new_item)
    await push_logs_to_cloudwatch(f"Item {new_item.id} created")
    return new_item

@app.get("/items/")
async def read_items():
    db = SessionLocal()
    items = db.query(Item).all()
    
    await push_logs_to_cloudwatch(f"Get all items. Total items: {len(items)}")
    
    return items

@app.get("/items/{item_id}")
async def read_item(item_id: int):
    db = SessionLocal()
    item = db.query(Item).filter(Item.id == item_id).first()
    if item is None:
        await push_logs_to_cloudwatch(f"Item {item_id} not found")
        raise HTTPException(status_code=404, detail="Item not found")
    
    await push_logs_to_cloudwatch(f"Item {item_id} retrieved")
    
    return item

@app.put("/items/{item_id}")
async def update_item(item_id: int, name: str, description: str):
    db = SessionLocal()
    item = db.query(Item).filter(Item.id == item_id).first()
    if item is None:
        
        await push_logs_to_cloudwatch(f"Item {item_id} not found")
        
        raise HTTPException(status_code=404, detail="Item not found")

    item.name = name
    item.description = description
    db.commit()
    
    await push_logs_to_cloudwatch(f"item {item_id} updated")
    
    return item

@app.delete("/items/{item_id}")
async def delete_item(item_id: int):
    db = SessionLocal()
    item = db.query(Item).filter(Item.id == item_id).first()
    if item is None:
        
        await push_logs_to_cloudwatch(f"Item {item_id} not found")
        
        raise HTTPException(status_code=404, detail="Item not found")

    db.delete(item)
    db.commit()
    
    await push_logs_to_cloudwatch(f"Item {item_id} deleted")
    
    return {"detail": "Item deleted successfully"}