'''
Tesouro Direto search info provider.
'''
import requests, urllib3

from fbpyutils import debug
from fbpyutils.datetime import apply_timezone

from typing import Dict
from datetime import datetime

urllib3.disable_warnings()


def treasury_bonds(x: str=None) -> Dict:
    """
    Retrieve information about treasury bonds from a specific source.
     Args:
    - x (str, optional): The name of the bond to retrieve information for. If not provided, information for all bonds is retrieved.
     Returns:
    - result (Dict): A dictionary containing information about the treasury bonds.
      - 'info' (str): Information about the type of bonds ('TREASURY BOND').
      - 'source' (str): The source of the bond information ('TESOURO DIRETO').
      - 'status' (str): The status of the retrieval process ('SUCCESS', 'NOT FOUND', or 'ERROR').
      - 'details' (Dict): Additional details about the bonds.
        - 'market' (Dict): Information about the market status and timings.
          - 'status' (str): The status of the market ('OPEN' or 'CLOSED').
          - 'closing_time' (datetime): The closing time of the market.
          - 'opening_time' (datetime): The opening time of the market.
          - 'position_time' (datetime): The time of the bond position.
        - 'matches' (int): The number of bonds that match the provided name (if any).
        - 'bonds' (List[Dict]): A list of dictionaries containing information about the matching bonds.
          - 'bond_name' (str): The name of the bond.
          - 'due_date' (datetime): The due date of the bond.
          - 'financial_indexer' (str): The financial indexer of the bond.
          - 'annual_investment_rate' (float): The annual investment rate of the bond.
          - 'annual_redemption_rate' (float): The annual redemption rate of the bond.
          - 'isin_code' (str): The ISIN code of the bond.
          - 'sell_price' (float): The sell price of the bond.
          - 'sell_price_unit' (float): The sell price unit of the bond.
          - 'buy_price' (float): The buy price of the bond.
          - 'buy_price_unit' (float): The buy price unit of the bond.
          - 'extended_description' (str): The extended description of the bond.
     Raises:
    - TypeError: If all ciphers fail to negotiate a secure connection.
    - SystemError: If there is an error getting information from the source.
    """
    h = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-control': 'no-cache'
    }

    u = "https://www.tesourodireto.com.br/json/br/com/b3/tesourodireto/service/api/treasurybondsinfo.json"

    result = {
        'info': 'TREASURY BOND',
        'source': 'TESOURO DIRETO',
        'status': 'SUCCESS',
        'details': {}
    }

    cipher = 'HIGH:!DH:!aNULL'
    r = None

    try:
        requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += cipher
        try:
            requests.packages.urllib3.contrib.pyopenssl.DEFAULT_SSL_CIPHER_LIST += cipher
        except AttributeError:
            # no pyopenssl support used / needed / available
            pass         

        r = requests.get(u, verify=False, headers=h)

        if not r:
            raise TypeError('All ciphers tryied to negotiate secure connection. No success at all.')

        data = r.json()

        if data.get('responseStatus') != 200:
            raise SystemError('Error getting information from source')

        response_data = data.get('response')

        response_market_data = response_data['TrsrBondMkt']

        response_business_data = response_data['BizSts']

        tz = 'America/Sao_Paulo'

        market_info = {
            'status': 'OPEN' if response_market_data['sts'] == 'Aberto' else 'CLOSED',
            'closing_time': apply_timezone(datetime.fromisoformat(response_market_data['clsgDtTm']), tz),
            'opening_time': apply_timezone(datetime.fromisoformat(response_market_data['opngDtTm']), tz),
            'position_time': apply_timezone(datetime.fromisoformat(response_business_data['dtTm']), tz)
        }

        bonds = [
            {
                'bond_name': b.get('TrsrBd', {}).get('nm'),
                'due_date': datetime.fromisoformat(b.get('TrsrBd', {}).get('mtrtyDt')),
                'financial_indexer': b.get('TrsrBd', {}).get('FinIndxs', {}).get('nm'), 
                'annual_investment_rate': b.get('TrsrBd', {}).get('anulInvstmtRate'),
                'annual_redemption_rate': b.get('TrsrBd', {}).get('anulRedRate'),
                'isin_code': b.get('TrsrBd', {}).get('isinCd'),
                'sell_price': b.get('TrsrBd', {}).get('untrRedVal'),
                'sell_price_unit': b.get('TrsrBd', {}).get('minRedVal'),
                'buy_price': b.get('TrsrBd', {}).get('untrInvstmtVal'),
                'buy_price_unit': b.get('TrsrBd', {}).get('minInvstmtAmt'),
                'extended_description': ' '.join([
                     str(b.get('TrsrBd', {}).get('featrs', 'NA')), 
                     str(b.get('TrsrBd', {}).get('invstmtStbl', 'NA'))
                ]).replace('\r\n', '').replace('NoneType', 'NA')
            } 
            for b in response_data.get('TrsrBdTradgList', {}) if b and (
                b.get('TrsrBd', {}).get('nm', 'NA') == x or x is None
            )
        ]

        if len(bonds) == 0:
            result['status'] = 'NOT FOUND'
            result['details'] = {
                'bond_name': x or 'ALL',
            }
            return result

        result['details'] = {
            'market': market_info,
            'matches': len(bonds),
            'bonds': bonds
        }

    except Exception as e:
        m =  debug.debug_info(e)
        result['status'] = 'ERROR'
        result['details'] = {
            'error_message': m
        }

    return result
