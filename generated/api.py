from enum import Enum
import dataset
from fastapi import FastAPI, Depends

db = dataset.connect("sqlite:///../database/hhdemo.db")

# Create the API
app = FastAPI()


@app.get("/")
def get_root():
    return {"/docs": "OpenAPI documentation", "/redoc": "Redoc documentation"}


@app.get("/10100116")
def get_data(limit: int = 10):
    data_table = db["10100116"]
    return list(data_table.find(_limit=limit))


@app.get("/10100116/columns")
def get_columns():
    data_table = db["10100116"]
    return data_table.columns

