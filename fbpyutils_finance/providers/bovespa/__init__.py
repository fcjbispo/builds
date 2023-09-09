from multiprocessing.sharedctypes import Value
import os
import requests
import pandas as pd
from datetime import datetime, timedelta

from fbpyutils import file as F
from fbpyutils_finance import providers

_bvmf_cert=providers.CERTIFICATES['bvmf-bmfbovespa-com-br']

class FetchModes:
    LOCAL = 0
    DOWNLOAD = 1
    LOCAL_OR_DOWNLOAD = 2
    STREAM = 3

class StockHistory():
    _col_widths = [
        2,
        8,
        2,
        12,
        3,
        12,
        10,
        3,
        4,
        13,
        13,
        13,
        13,
        13,
        13,
        13,
        5,
        18,
        18,
        13,
        1,
        8,
        7,
        13,
        12,
        3
    ]

    _col_names = [
        'record_type',
        'trade_date',
        'bdi_code',
        'ticker',
        'market_type',
        'ticker_issuer',
        'ticker_specs',
        'term_days',
        'currency',
        'open_value',
        'max_value',
        'min_value',
        'average_value',
        'close_value',
        'best_buy_offer',
        'best_sell_offer',
        'total_trades',
        'total_trades_papers',
        'total_trades_value',
        'option_market_current_price',
        'option_market_current_price_adjustment_indicator',
        'option_market_due_date',
        'ticker_trade_factor',
        'option_market_current_price_in_points',
        'ticker_isin_code',
        'ticker_distribution_number'
    ]

    _converters = {
        'record_type': lambda x: str(int(x)),
        'trade_date': lambda x: str(x),
        'bdi_code': lambda x: str(x),
        'ticker': lambda x: str(x),
        'market_type': lambda x: str(int(x)),
        'ticker_issuer': lambda x: str(x),
        'ticker_specs': lambda x: str(x),
        'term_days': lambda x: str(x),
        'currency': lambda x: str(x),
        'open_value': lambda x: str(int(x)),
        'max_value': lambda x: str(int(x)),
        'min_value': lambda x: str(int(x)),
        'average_value': lambda x: str(int(x)),
        'close_value': lambda x: str(int(x)),
        'best_buy_offer': lambda x: str(int(x)),
        'best_sell_offer': lambda x: str(int(x)),
        'total_trades': lambda x: str(int(x)),
        'total_trades_papers': lambda x: str(int(x)),
        'total_trades_value': lambda x: str(int(x)),
        'option_market_current_price': lambda x: str(int(x)),
        'option_market_current_price_adjustment_indicator': lambda x: str(int(x)),
        'option_market_due_date': lambda x: str(int(x)),
        'ticker_trade_factor': lambda x: str(int(x)),
        'option_market_current_price_in_points': lambda x: str(int(x)),
        'ticker_isin_code': lambda x: str(x),
        'ticker_distribution_number': lambda x: str(x)
    }

    _data_columns = [
        'trade_date',
        'bdi_code',
        'market_type',
        'ticker',
        'ticker_issuer',
        'ticker_specs',
        'ticker_isin_code',
        'term_days',
        'currency',
        'open_value',
        'min_value',
        'max_value',
        'average_value',
        'close_value',
        'total_trades',
        'total_trades_papers',
        'total_trades_value' 
    ]

    _original_col_names = [
        'tipreg',
        'datpre',
        'codbdi',
        'codneg',
        'tpmerc',
        'nomres',
        'especi',
        'prazot',
        'modref',
        'preabe',
        'premax',
        'premin',
        'premed',
        'preult',
        'preofc',
        'preofv',
        'totneg',
        'quatot',
        'voltot',
        'preexe',
        'indopc',
        'datven',
        'fatcot',
        'ptoexe',
        'codisi',
        'dismes'
    ]

    _original_data_columns = [
        'datpre',
        'codbdi',
        'tpmerc',
        'codneg',
        'nomres',
        'especi',
        'codisi',
        'prazot',
        'modref',
        'preabe',
        'premin',
        'premax',
        'premed',
        'preult',
        'totneg',
        'quatot',
        'voltot'
    ]

    @staticmethod
    def to_float(x): 
        return float('.'.join([x[0:-2], x[-2:]])) if type(x) == str else float(x)

    @staticmethod
    def to_date(x, format='%Y%m%d'): 
        return pd.to_datetime(x, format=format, errors='ignore').date()

    def __init__(self, download_folder=None):
        if download_folder is None or len(download_folder) == 0:
            download_folder = os.path.expanduser('~')
        
        if not os.path.exists(download_folder):
            raise OSError('Path doesn\'t exists.')

        if not os.path.isdir(download_folder):
            raise OSError('Path is not a folder.')

        self.download_folder = download_folder 

    def _build_paths(self, period='A', period_data=None):
        period = period or 'A' 
        
        if period is None or period not in ['A','M','D']:
            raise ValueError('Invalid period. User A, M or D.')
        
        if period == 'A':
            period_data = period_data or datetime.today().strftime('%Y')
        elif period == 'M':
            period_data = period_data or datetime.today().strftime('%m%Y')
        else:
            yesterday = datetime.today() - timedelta(days=1)
            period_data = period_data or yesterday.strftime('%d%m%Y')

        cotfile='COTAHIST_' + period + period_data + '.ZIP'

        url='https://bvmf.bmfbovespa.com.br/InstDados/SerHist/' + cotfile

        output_file = os.path.sep.join([self.download_folder, cotfile])
        
        return url, output_file

    def _download_stock_history(self, period='A', period_data=None):
        url, output_file = self._build_paths(period, period_data)

        block_size = 1024**3
        response = requests.get(url, stream=True, verify=_bvmf_cert)
        with open(output_file, "wb") as handle:
            for data in response.iter_content(block_size):
                handle.write(data)
                
        return output_file

    def _treat_data(self, data, original_names, compact):
        cot_data = data[data['record_type'] == '1'].copy(deep=True)

        for col in [
            'open_value', 'min_value', 'max_value', 'average_value', 
            'close_value', 'total_trades_value'
        ]:
            cot_data[col] = cot_data[col].apply(StockHistory.to_float)
        
        cot_data['trade_date'] = cot_data['trade_date'].apply(StockHistory.to_date)
        
        cot_data = cot_data.fillna(0)

        if original_names:
            cot_data.columns = self._original_col_names
            if compact:
                return cot_data[self._original_data_columns]
            else:
                return cot_data
        else:
            if compact:
                return cot_data[self._data_columns]
            else:
                return cot_data


    def _check_local_history(self, period='A', period_data=None):
        _, local_file = self._build_paths(period, period_data)

        return (
            os.path.exists(local_file) and 
            os.path.isfile(local_file) and 
            F.mime_type(local_file) == 'application/zip'
        )

    def get_stock_history(
        self, period='A', period_data=None, fetch_mode=FetchModes.LOCAL_OR_DOWNLOAD, 
        compact=True, original_names=False
    ):
        if not fetch_mode in [
            FetchModes.LOCAL, FetchModes.DOWNLOAD, FetchModes.LOCAL_OR_DOWNLOAD, 
            FetchModes.STREAM
        ]:
            raise ValueError('Invalid fetch mode.')
    
        if fetch_mode == FetchModes.LOCAL_OR_DOWNLOAD:
            if self._check_local_history(period, period_data):
                fetch_mode = FetchModes.LOCAL
            else:
                fetch_mode = FetchModes.DOWNLOAD
            
        if fetch_mode == FetchModes.DOWNLOAD:
            data_file = self._download_stock_history(period, period_data)

        if fetch_mode == FetchModes.LOCAL:
            if self._check_local_history(period, period_data):
                _, data_file = self._build_paths(period, period_data)
            else:
                raise OSError('Invalid or non existent local file.')

        if fetch_mode == FetchModes.STREAM:
            data_file, _ = self._build_paths(period, period_data)

        cot = None
        cot = pd.read_fwf(
            data_file, header = None, compression = 'zip', widths=self._col_widths, 
            names=self._col_names, converters = self._converters
        )
        
        if type(cot) != pd.core.frame.DataFrame:
            raise TypeError('Failed to get the stock history. Invalid output data.')
        
        return self._treat_data(cot, original_names, compact)

