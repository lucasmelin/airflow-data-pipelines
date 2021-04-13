import dataset
import csv
import os
from pathlib import Path


def load(ti):
    # Creates the database if it doesn't exist
    db = dataset.connect("sqlite:////opt/airflow/database/hhdemo.db")
    name = Path(ti.xcom_pull(task_ids='get_filename'))
    
    # Get a reference to the table 'data'
    table = db.create_table(name.stem, primary_id='dataid')

    # Insert all the records in the csv
    with name.open() as csvfile:
        reader = csv.DictReader(csvfile)
        table.insert_many(list(reader))
    

def get_latest_file(filepath):
    list_of_files = Path(filepath).glob('*')
    latest_file = max(list_of_files, key=os.path.getctime)
    return str(latest_file)
