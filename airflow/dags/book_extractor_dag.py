from airflow.decorators import dag, task
from datetime import datetime
from typing import Dict,Any
from pathlib import Path
import pandas as pd
import os
import sys
sys.path.append(str(Path(__file__).resolve().parents[1]))
from pipeline import Extractor,DataTransformer
from utils import load_yaml


@dag(dag_id='book_extractor_dag',
     schedule=None,
     max_active_tasks=1,
     start_date=datetime(2025,6,1),
     catchup=False,
     tags=['book_extractor'],
     params={"config_path":Path(__file__).resolve().parents[1] / "config" / "config.yaml",
             "save_path":Path(__file__).resolve().parents[1] / "data"})

def book_extractor_dag():

    @task
    def extract_books()->list:
        books_data = Extractor().scrap_books()
        return books_data
    
    @task
    def transform_books_data(books_data:list,**kwargs):
        os.makedirs(kwargs['params']['save_path'],exist_ok=True)
        config_path= kwargs['params']['config_path']
        print(f"Loading transformation config from: {config_path}")
        # with open(config_path,'r')  as file:
        #     config = load_yaml(file)['transformation']
        config = load_yaml(config_path)['transformation']
        dataframe= pd.DataFrame(books_data)
        transformer_instantiate = DataTransformer()
        transformed_book_data = transformer_instantiate.transform_ratings(dataframe,config['rating_map']).pipe(transformer_instantiate.price_transformation).pipe(transformer_instantiate.scraped_date).pipe(transformer_instantiate.add_uuid)
        transformed_book_data.to_parquet(os.path.join(kwargs['params']['save_path'],f"books_data_{transformer_instantiate.date}.parquet"),index=False,engine='fastparquet')

    books_data = extract_books()
    transform_books_data(books_data)

book_extractor_dag()