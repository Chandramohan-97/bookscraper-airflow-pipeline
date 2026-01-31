import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import logging
from typing import List,Dict,Any,Union
import urllib
from utils import load_yaml
from pathlib import Path


class BookToScrapePipeline:

    def __init__(self):
        BASE_DIR = Path(__file__).resolve().parents[1]
        CONFIG_PATH = BASE_DIR / "config" / "config.yaml"
        config = load_yaml(CONFIG_PATH)

        self.base_url = config['executor']['url']
        self.session = requests.Session()
        self.headers = config['executor']['headers']        
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.INFO)
        self.log.propagate = True

    def _get_values(self, attributes: BeautifulSoup) -> Union[Dict[str,Any],None]:
        """Extracts the required values from the book attributes section.
        
        Args:
            attributes (BeautifulSoup): The BeautifulSoup object containing book attributes.
            
        Returns:
            Dict[str, Any]: A dictionary containing the extracted values.
            
        """
        try:
            stars = ratings["class"][1] if (ratings := attributes.find("p",class_="star-rating")) else None
        
            title= name["title"] if (name :=attributes.find("h3").find('a')) else None

            class_price = product[0] if (product := attributes.select("div[class='product_price']")) else None
            if class_price:
                price = book_price.get_text() if (book_price := class_price.find('p',class_="price_color")) else None
                availability = book_availability.get_text().strip() if (book_availability := class_price.find('p',class_="availability")) else None
                return {'ratings':stars,'title':title,
                        'price':price,
                        'availability':availability}
            return {'ratings':stars,'title':title,
                    'price':None,
                    'availability':None}
        except Exception as e:
            self.log.exception(f"Error in extracting values: {e}")
            return None
    
    def scrap_books(self)-> List[Dict[str,Any]]:
        
        books_list =[]
        url = self.base_url
        while True:
            contents,next_page = self.__get_data(url)
            books_list.extend(contents)
            # print(next_page)
            if next_page:
                url = next_page
            else:
                break
            
        return books_list
    
    def __get_data(self,url:str) -> Union[List[Dict[str,Any]],None]:
        """Fetches and parses book data from the given URL."""
        try:
            response = self.session.get(url,headers=self.headers)
            soup= BeautifulSoup(response.content, 'html.parser')
            books = soup.select("article[class='product_pod']")
            results = []
            for book in books:
                book_data = self._get_values(book)
                if book_data:
                    results.append(book_data)

            next_page = pages.find('a')['href'] if (pages :=soup.find("li",class_="next")) else None
            # print(next_page)
            if next_page:
                parsed = urllib.parse.urlparse(url)
                next_page = urllib.parse.urlunparse((parsed.scheme,parsed.netloc,parsed.path.strip('/').replace(parsed.path.strip('/').split('/')[-1],next_page), '', '', ''))
                
            return results,next_page
        except Exception as e:
            self.log.exception(f"Error in getting data from url {url}: {e}")
            return [],None