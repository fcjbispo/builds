'''
Data Providers: BOVESPA Package.
'''
import os
import requests
import pandas as pd
from datetime import datetime, timedelta

import fbpyutils_finance as FI
from fbpyutils import file as F, xlsx as XL


_bvmf_cert=FI.CERTIFICATES['bvmf-bmfbovespa-com-br']


class FetchModes:
    '''
    - Description: The  `FetchModes`  class defines different fetch modes for data retrieval.
    - Functionality: This class provides constants representing different fetch modes.
    - Attributes:
    -  `LOCAL`  (int): Represents the fetch mode for local data retrieval. Value is  `0` .
    -  `DOWNLOAD`  (int): Represents the fetch mode for downloading data. Value is  `1` .
    -  `LOCAL_OR_DOWNLOAD`  (int): Represents the fetch mode for either local data retrieval or downloading data. Value is  `2` .
    -  `STREAM`  (int): Represents the fetch mode for streaming data. Value is  `3` .
    '''
    LOCAL = 0
    DOWNLOAD = 1
    LOCAL_OR_DOWNLOAD = 2
    STREAM = 3


class StockHistory():
    '''
    - Description: The  `class StockHistory`  class fetches stock history data from BOVESPA.
    - Functionality: This class provides methods to download historical data using different periods and fetch modes and
        return historical data as pandas dataframes.
    - Attributes:
    -  `download_folder`  (str): Local folder for data storage. Used do retrieve already downloaded data and/or 
            update files with latest historic data. The data are stored as ZIP
    '''
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
    def validate_period_date(period_date):
        """
        Validates the given period date against supported formats.
        Args:
            period_date (str): The period date to be validated.
        Returns:
            bool: Returns True if the date is valid, False otherwise.
        Raises:
            ValueError: If the provided date format or value is invalid.
        Supported date formats:
            - '%Y%m': Year and month format (e.g., '202201')
            - '%Y%m%d': Year, month, and day format (e.g., '20220101')
        """
        formats = ['%Y%m', '%Y%m%d']  # Supported date formats

        for date_format in formats:
            try:
                datetime.strptime(period_date, date_format)
                return True  # Date is valid
            except ValueError:
                pass

        raise ValueError("Invalid date format or value: " + period_date)


    @staticmethod
    def to_float(x):
        """
        Converts a value to a float.
        Args:
            x (str or any): The value to be converted to float.
        Returns:
            float: The converted float value of x.
        Overview:
        This method is used to convert a value to a float. 
        If the value is a string, it joins the characters before the last two characters 
        with a dot to create a decimal representation. 
        If the value is not a string, it directly converts it to a float.
        """
        return float('.'.join([x[0:-2], x[-2:]])) if type(x) == str else float(x)


    @staticmethod
    def to_date(x, format='%Y%m%d'):
        """
        Converts a value to a date object.
        Args:
            x (str or any): The value to be converted to a date object.
            format (str): Optional. The format of the input value. Default is '%Y%m%d'.
        Returns:
            datetime.date: The converted date object.
        Overview:
        This method is used to convert a value to a date object using the specified format. 
        It utilizes the pd.to_datetime() function from the pandas library to perform the conversion.
        """
        return pd.to_datetime(x, format=format, errors='ignore').date()

    @staticmethod
    def get_info_tables():
        info_tables_path = os.path.sep.join([
            FI.APP_FOLDER, 'bovespa', 'tabelas_anexas_bovespa.xlsx'])
        response = {
            'status': 'OK'
        }
        try:
            info_tables = XL.ExcelWorkbook(info_tables_path)
            response['tables'] = {}
            for sheet in info_tables.sheet_names:
                info_data = tuple(info_tables.read_sheet(sheet))
                response['tables'][sheet] = pd.DataFrame(
                    info_data[1:], 
                    columns=[c.lower() for c in info_data[0]]
                )
            response['message'] = f'All {len(info_tables.sheet_names)} fetched.'
        except Exception as e:
            response['status'] = 'ERROR'
            response['message'] = f'Error fetching bovespa info tables: {str(e)}'

        return response

    def __init__(self, download_folder=None):
        """
        Constructor method for a class.
        Args:
            download_folder (str): Optional. The path to the download folder. Default is None.
        Raises:
            OSError: If the download_folder path is invalid.
        Overview:
        This method is used to initialize an instance of the class. 
        It sets the download_folder attribute based on the provided argument or the default value. 
        It also performs some validations on the download_folder path to ensure it exists and is a folder. 
        If the path is invalid, it raises an OSError with an appropriate error message.
        """
        if download_folder is None or len(download_folder) == 0:
            download_folder = os.path.expanduser('~')

        if not os.path.exists(download_folder):
            raise OSError('Path doesn\'t exists.')

        if not os.path.isdir(download_folder):
            raise OSError('Path is not a folder.')

        self.download_folder = download_folder


    def _build_paths(self, period='A', period_date=None):
        """
        Builds the paths required for downloading historical data files.
        Args:
            period (str, optional): The period for which data is requested. Defaults to 'A'.
                                    Possible values are 'A' for annual, 'M' for monthly, or 'D' for daily.
            period_date (str or datetime, optional): The date for which data is requested. Defaults to None.
                                                    Format must be '%Y' for annual, '%m%Y' for monthly,
                                                    or '%d%m%Y' for daily.
        Raises:
            ValueError: If an invalid period is provided.
        Returns:
            tuple: A tuple containing the URL for downloading the data file and the output file path.
        Overview:
        This function builds the URL and output file path required for downloading historical data files.
        It takes the period and period_date as inputs, and based on their values, constructs the cotfile name,
        URL, and output file path.
        If the period is 'A' (annual), the period_date is set to the current year if not provided.
        If the period is 'M' (monthly), the period_date is set to the current month and year if not provided.
        If the period is 'D' (daily), the period_date is set to yesterday's date if not provided.
        The cotfile name is constructed by concatenating the period and period_date with 'COTAHIST_' prefix
        and '.ZIP' suffix.
        The URL is constructed by appending the cotfile name to the base URL 
        'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/'.
        The output file path is constructed by joining the download folder path with the cotfile name.
        The function returns a tuple containing the URL and output file path.
        """
        period = period or 'A'

        if period is None or period not in ['A', 'M', 'D']:
            raise ValueError('Invalid period. User A, M or D.')
        
        if period_date:
            self.validate_period_date(period_date=period_date)

        if period == 'A':
            period_date = period_date or datetime.today().strftime('%Y')
        elif period == 'M':
            period_date = period_date or datetime.today().strftime('%m%Y')
        else:
            yesterday = datetime.today() - timedelta(days=1)
            period_date = period_date or yesterday.strftime('%d%m%Y')

        cotfile = 'COTAHIST_' + period + period_date + '.ZIP'

        url = 'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/' + cotfile

        output_file = os.path.sep.join([self.download_folder, cotfile])

        return url, output_file


    def _download_stock_history(self, period='A', period_data=None):
        """
        Downloads the stock history data file.
        Args:
            period (str, optional): The period for which data is requested. Defaults to 'A'.
                                    Possible values are 'A' for annual, 'M' for monthly, or 'D' for daily.
            period_data (str or datetime, optional): The date for which data is requested. Defaults to None.
                                                    Format must be '%Y' for annual, '%m%Y' for monthly,
                                                    or '%d%m%Y' for daily.
        Returns:
            str: The path of the downloaded stock history data file.
        Overview:
        This function downloads the stock history data file from a given URL and saves it to the specified output file path.
        It takes the period and period_data as inputs and uses them to build the URL and output file path by calling the
        `_build_paths` method.
        The block_size variable is set to 1 GB (1024**3) for streaming the response content in chunks.
        The requests library is used to send a GET request to the URL with streaming enabled and certificate verification.
        The response content is iterated in chunks, and each chunk is written to the output file using the handle.
        This allows downloading large files in a memory-efficient manner.
        Finally, the function returns the path of the downloaded stock history data file.
        """
        url, output_file = self._build_paths(period, period_data)

        block_size = 1024**3

        response = requests.get(url, stream=True, verify=_bvmf_cert)

        with open(output_file, "wb") as handle:
            for data in response.iter_content(block_size):
                handle.write(data)

        return output_file


    def _treat_data(self, data, original_names, compact):
        """
        Treats the provided data by applying transformations and returning the processed data.
        Args:
            data (pandas.DataFrame): The input data to be treated.
            original_names (bool): Flag indicating whether to use original column names or not.
            compact (bool): Flag indicating whether to return compact data or not.
        Returns:
            pandas.DataFrame: The treated data.
        Functionality:
        - Filters the data to include only records with 'record_type' equal to '1'.
        - Applies transformations to specific columns using the 'to_float' and 'to_date' functions.
        - Fills any missing values with 0.
        - If 'original_names' is True, assigns original column names to the treated data.
        - If 'compact' is True, returns a subset of columns based on either original or modified column names.
        - Otherwise, returns the entire treated data.
        """
        cot_data = data[data['record_type'] == '1'].copy(deep=True)

        for col in [
            'open_value', 'min_value', 'max_value', 'average_value',
            'close_value', 'total_trades_value'
        ]:
            cot_data[col] = cot_data[col].apply(StockHistory.to_float)

        cot_data['trade_date'] = cot_data['trade_date'].apply(
            StockHistory.to_date)

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
        """
        Checks if the local history file exists and is a valid zip file for the specified period and period data.
        Args:
            period (str, optional): The period for which the local history is checked. Default is 'A'.
            period_data (str, optional): The specific period data to be checked. Default is None.
        Returns:
            bool: True if the local history file exists, is a file, and has a mime type of 'application/zip'. False otherwise.
        Functionality:
        - Builds the local file path using the specified period and period data.
        - Checks if the local file exists, is a file, and has a mime type of 'application/zip'.
        - Returns True if all the conditions are met, indicating that the local history file is valid.
        - Returns False otherwise.
        """
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
        """Fetches stock history data based on the specified parameters.
        Args:
            period (str, optional): The period for which to fetch the stock history. Defaults to 'A'.
            period_data (str or datetime, optional): Additional data specifying the period. Defaults to None.
            fetch_mode (FetchModes, optional): The mode for fetching the data. Defaults to FetchModes.LOCAL_OR_DOWNLOAD.
            compact (bool, optional): Flag for compact data format. Defaults to True.
            original_names (bool, optional): Flag for using original column names. Defaults to False.
        Returns:
            pandas.core.frame.DataFrame: The fetched stock history data.
        Raises:
            ValueError: If an invalid fetch mode is provided.
            OSError: If the local file is invalid or non-existent.
            UnicodeDecodeError: If there is an error reading the stock history file with an unknown encoding.
            TypeError: If the output data is not a pandas DataFrame.
        Note:
            - `FetchModes` is an enum representing different modes for fetching data.
            - The method internally handles different fetch modes to determine how to fetch the stock history data.
            - It also handles encoding-related errors while reading the data file.
            - The fetched data is treated and returned in a pandas DataFrame format.
        """
        if fetch_mode not in [
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
        encoding_list = ['ISO-8859-1', 'cp1252', 'latin', 'utf-8']
        while cot is None and len(encoding_list) > 0:
            encoding = encoding_list.pop()
            try:
                cot = pd.read_fwf(
                    data_file, header=None, compression='zip', widths=self._col_widths,
                    names=self._col_names, converters=self._converters, encoding=encoding
                )
            except UnicodeDecodeError as e:
                cot = None

        if cot is None:
            raise UnicodeDecodeError(
                "Error reading stock history file. Unknown encoding.")

        if type(cot) != pd.core.frame.DataFrame:
            raise TypeError(
                'Failed to get the stock history. Invalid output data.')

        return self._treat_data(cot, original_names, compact)
