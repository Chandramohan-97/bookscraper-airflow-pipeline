import pandas as pd
from datetime import datetime
import uuid
import logging
from typing import Dict,Any

class DataTransformer:
    
    
    def __init__(self,**kwargs):
        self.date = kwargs.get('date',datetime.now().strftime("%Y%m%d"))
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.INFO)
        self.log.propagate = True
    
    def transform_ratings(self,df:pd.DataFrame,rating_map:Dict[str,Any])-> pd.DataFrame:
        """
        Convert ratings from words to numerical values based on the provided mapping.
        
        :param df: DataFrame containing a 'ratings' column with word-based ratings.
        :type df: pd.DataFrame
        :param rating_map: Dictionary mapping word ratings to numerical values.
        :type rating_map: Dict[str, Any]
        :return: DataFrame with transformed 'ratings' column.
        :rtype: DataFrame
        """
        try:
            assert 'ratings' in df.columns, "ratings column not found in dataframe"
        
            df_up = df.assign(**{'ratings':lambda file: file['ratings'].map(rating_map)})
            return df_up
        except AssertionError as e:
            self.log.exception(f"Error in transforming ratings: {e}")
            raise
        except Exception as e:
            self.log.exception(f"Unexpected error in transforming ratings: {e}")
            raise
    
    def price_transformation(self,df:pd.DataFrame)-> pd.DataFrame:
        """
        Remove currency symbol from 'price' column and convert it to float.
        
        :param df: DataFrame containing a 'price' column with currency symbols.
        :type df: pd.DataFrame
        :return: DataFrame with transformed 'price' column.
        :rtype: DataFrame
        """
        try:
            assert 'price' in df.columns, "price column not found in dataframe"
            df_up = df.assign(**{'price':lambda file: file['price'].str.replace('£','').astype(float)})
            df_up.rename(columns={'price':'price in pounds'},inplace=True,errors='ignore')
            return df_up
        except AssertionError as e:
            self.log.exception(f"Error in transforming price: {e}")
            raise   
        except Exception as e:
            self.log.exception(f"Unexpected error in transforming price: {e}")
            raise
    
    def scraped_date(self,df:pd.DataFrame)-> pd.DataFrame:
        try:
            df_up = df.assign(**{'scraped_date':self.date})
            return df_up
        except Exception as e:
            self.log.exception(f"Error in adding scraped date: {e}")
            raise
    
    def add_uuid(self,df:pd.DataFrame)-> pd.DataFrame:
        try:
            df_up = df.assign(**{'uuid':[str(uuid.uuid4()) for _ in range(len(df))]})
            return df_up
        except Exception as e:
            self.log.exception(f"Error in adding uuid: {e}")
            raise