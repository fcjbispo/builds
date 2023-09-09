# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.7
#   kernelspec:
#     display_name: fbpyutils-finance--TrezB8H
#     language: python
#     name: python3
# ---

import sys
sys.path.insert(0, '..')

# +
from fbpyutils import debug

from fbpyutils.datetime import apply_timezone

from typing import Dict
import requests 
import datetime
from bs4 import BeautifulSoup

from fbpyutils_finance import MARKET_INFO, first_or_none, numberize, random_header


# -

def _makeurl(x):
    '''
        Build default Google search URL output.
        Parameters:
            x (str): The search query string
        Returns:
            str: A string with a full Google search URL from the search query.
    '''    
    q = x.upper()
    url = f"https://finance.yahoo.com/quote/{q}/"
    return url


def _ysearch(x: str) -> requests.models.Response:
    '''
        Performs a default Yahoo search using custom headers.
        Parameters:
            x (str): The search query string
        Returns:
            http response: An HTTP response with the HTML page resulting from the search query.
    '''
    h = random_header()

    s = requests.Session()
    url = _makeurl(x)
    r = s.get(url, headers=h)

    return r


def stock_price(
    x: str, market: str=None
) -> Dict:
    '''
        Performs a Yahoo search for the current price of the supplied ticker in the default market.
        Parameters:
            x (str): The ticker to search for the current price.
            market (str, Optional): The name of the market on which the ticker will be searched.
        Returns:
            dict: A standard dictionary with the stock price and information for the supplied ticker.
    '''
    result = {
        'info': 'STOCK PRICE',
        'source': 'YAHOO',
        'status': 'SUCCESS',
        'details': {}
    }

    step = 'Init'
    try:
        if not x:
            raise ValueError('Ticker is required')

        ticker = x.upper()

        response = _ysearch(ticker)

        if response.status_code != 200:
            raise ValueError(f'Yahoo Search Fail: {search}, {response.status_code}')

        soup = BeautifulSoup(response.text, "html.parser")

        step = 'Search: ticker name, market, currency'

        element1 = soup.find('div', id='mrt-node-Lead-5-QuoteHeader')
        element2 = element1.find('h1', class_='D(ib) Fz(18px)')
        element3 = element1.find('div', class_='C($tertiaryColor) Fz(12px)')

        if all([element2, element3]):
            ticker_name, ticker = [e.strip() for e in element2.text.replace(')', '').split('(')]

            elements = [e.upper() for e in element3.text.replace(' ', '|').split('|')]
            market, currency = elements[0], elements[-1]
        else:
            raise ValueError(f'Yahoo Search Fail on step {step}!')

        step = 'Search: price'
        element4 = element1.find('fin-streamer', class_='Fw(b) Fz(36px) Mb(-4px) D(ib)')

        if all([element4]):
            price = float(element4.text)
        else:
            raise ValueError(f'Yahoo Search Fail on step {step}!')

        result['details'] = {
            'market': market,
            'ticker': ticker,
            'name': ticker_name,
            'currency': currency,
            'price': price,
            'variation': None,
            'variation_percent': None,
            'trend': None,
            'position_time':  datetime.datetime.now()
        }
    except Exception as e:
        print(e, step)
        m =  debug.debug_info(e)
        result['status'] = 'ERROR'
        result['details'] = {
            'error_message': m
        }
    
    return result


ticker = 'ICAP'

stock_price(ticker)


