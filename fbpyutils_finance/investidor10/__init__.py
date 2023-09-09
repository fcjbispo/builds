import os
import sys
import time
import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from bs4.element import Tag
from datetime import datetime
from multiprocessing import Pool

from fbpyutils import file as FU


PARALLELIZE = True

BS4_PARSER = 'lxml'

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1667.0 Safari/537.36'}

FIIS_COM_URL = 'https://investidor10.com.br/fiis/dividendos/'
FIIS_PAYMENT_URL = 'https://investidor10.com.br/fiis/dividendos/data_pgto/'
FIIS_DY_DETAILS_URL = 'https://investidor10.com.br/fiis/rankings/maior-dividend-yield/'
IFIX_PAGE_URL = "https://fiis.com.br/ifix/"

CAPTURE_DATE = datetime.now().date()


def _tag_to_str(x):
    """
    Convert a given tag object to a string representation.
     Args:
        x (Tag): The tag object to convert.
     Returns:
        str: The modified text representation of the tag object.
     Description:
        This lambda function takes a tag object as input and converts it to a string representation.
        It removes any newline characters from the text and removes leading and trailing whitespace.
        The resulting modified text is then returned as a string.
    """
    return x.text.replace('\n', '').strip()


def _any_to_number(x):
    """
    Convert a given value to a numeric representation.
     Args:
        x (any): The value to convert.
     Returns:
        float or None: The numeric representation of the value, or None if the value is '-'.
     Description:
        This lambda function takes a value as input and converts it to a numeric representation.
        If the value is '-', it returns None.
        Otherwise, it performs the following transformations on the value:
        - Split the value by spaces and take the last element.
        - Replace any occurrence of '.' with an empty string.
        - Replace any occurrence of ',' with '.'.
        - Replace any occurrence of '%' with an empty string.
        The resulting modified value is then converted to a float and returned.
    """
    return None if str(x) == '-' \
        else float(str(x).split(' ')[-1].replace('.','').replace(',','.').replace('%',''))


def _get_fii_all_payment_data(fiis_page_url):
    """
    Retrieve and process payment data for FIIs (Fundos de Investimento Imobiliário).
     Args:
        fiis_page_url (str): The URL of the FIIs page to scrape.
     Returns:
        pandas.DataFrame: A DataFrame containing the payment data for FIIs.
     Raises:
        SystemError: If the HTTP request to the FIIs page returns a non-200 status code.
     Description:
        This function retrieves payment data for FIIs from a given webpage URL. It scrapes the webpage using the requests library
            and parses the HTML content using BeautifulSoup.
        The function starts by checking the HTTP status code of the webpage. If the status code is not 200, it raises a 
            SystemError with an appropriate message.
        It then initializes an empty list to store the payment data and defines a list of month names in Portuguese.
        The current year is obtained using the datetime module.
        The function iterates over each month group of payments on the webpage and extracts the year from the month group's header
            if available.
        For each payment card within a month group, it retrieves the payment day, month, and other payment details.
        The function then extracts the FII ticker, details URL, name, payment amount, and payment date from each payment row within
            a payment card.
        The payment date is constructed using the extracted year, month, and day.
        The extracted data is appended to the 'data' list.
        Finally, the 'data' list is used to create a pandas DataFrame with appropriate column names.
        The resulting DataFrame is returned as the output of the function.
    """
    fiis_page = requests.get(fiis_page_url, headers=HEADERS)
    
    if fiis_page.status_code != 200:
        raise SystemError(f'Http Error not 200: {fiis_page.status_code}')
    
    P = BeautifulSoup(fiis_page.text, BS4_PARSER)

    data = []
    months = [
        'Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 
        'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'
    ]
    
    fii_pmt_year = datetime.now().year
    for fii_month_group in P.find('div', attrs={'id': 'list-content'}).findAll('div', attrs=({'class': 'month-group-payment'})):
        if fii_month_group.find('h3', attrs={'class': 'month-name'}):
            fii_pmt_year = int(_tag_to_str(fii_month_group.find('h3', attrs={'class': 'month-name'})).split(' ')[-1])
        
        for payment_card in fii_month_group.findAll('div', attrs={'class': 'payment-card'}):
            fii_pmt_day = int(_tag_to_str(payment_card.find('div', attrs={'class': 'payment-day'})))
            fii_pmt_month = months.index(_tag_to_str(payment_card.find('div', attrs={'class': 'text-center'}))) + 1
            
            for fii_payment in payment_card.find_all('div', attrs={'class': 'row payment-row'}):
                fii_ticker = _tag_to_str(fii_payment.find('a', attrs={'class': 'fii-ticker'}, href=True))
                fii_details = fii_payment.find('a', attrs={'class': 'fii-ticker'}, href=True)['href']    
                fii_name = _tag_to_str(fii_payment.find_all('h4')[-1])

                p1, p2 = fii_payment.find_all('p')
                fii_pmt = float(_tag_to_str(p1).split(' ')[-1])
                fii_com_date = datetime.strptime(p2.text.split(' ')[-1], '%d/%m/%Y')

                fii_pmt_date = datetime(fii_pmt_year, fii_pmt_month, fii_pmt_day).date()

                data.append([
                    fii_ticker, fii_name, fii_pmt, fii_com_date, fii_pmt_date, fii_details, CAPTURE_DATE
                ])
 
    fii_payment_dates = pd.DataFrame(
        data, columns=['ticker', 'name', 'payment', 'com_date', 'payment_date', 'details', 'reference_date']
    )

    return fii_payment_dates


def _get_fii_payment_data(fiis_page_url, type):
    """
    Retrieves FII payment data from a given webpage URL.
     Parameters:
        - fiis_page_url (str): The URL of the page containing the FII payment data.
        - type (str): The type of payment ('com' or 'payment').
     Returns:
        - fii_payment_dates (DataFrame): A pandas DataFrame containing the FII payment data.
     Raises:
        - TypeError: If the provided payment type is not valid.
     Overall:
        This function takes a URL and payment type as input, fetches the FII payment data from the webpage,
        extracts relevant information, and returns it as a pandas DataFrame.
    """
    if not type in ('com', 'payment'):
        raise TypeError('Invalid payment type')

    fiis_page = requests.get(fiis_page_url, headers=HEADERS)

    S = BeautifulSoup(fiis_page.text, BS4_PARSER)
    fii_payment_rows = S.find_all('div', attrs={'class': 'row payment-row'})

    fii_payments = []
    for fii_payment in fii_payment_rows:
        fii_ticker = _tag_to_str(fii_payment.find('a', attrs={'class': 'fii-ticker'}, href=True))
        fii_details = fii_payment.find('a', attrs={'class': 'fii-ticker'}, href=True)['href']    
        fii_name = _tag_to_str(fii_payment.find_all('h4')[-1])

        p1, p2 = fii_payment.find_all('p')
        fii_payment = float(_tag_to_str(p1).split(' ')[-1])

        fii_payment_date = datetime.strptime(p2.text.split(' ')[-1], '%d/%m/%Y')

        fii_payments.append((fii_ticker, fii_name, fii_payment, fii_payment_date, fii_details, CAPTURE_DATE))

    fii_payment_dates = pd.DataFrame(
        fii_payments, columns=['ticker', 'name', 'payment', f'{type}_date', 'details', 'reference_date']
    )

    return fii_payment_dates


def _get_ifix_data(ifix_page_url):
    """
    Retrieves iFIX data from a given webpage URL.
    Parameters:
        - ifix_page_url (str): The URL of the page containing the iFIX data.
     Returns:
        - ifix_data (DataFrame): A pandas DataFrame containing the iFIX data.
    Overall:
        This function takes a URL as input, fetches the iFIX data from the webpage, extracts relevant
        information, and returns it as a pandas DataFrame.
    """
    ifix_page = requests.get(ifix_page_url, headers=HEADERS)

    I = BeautifulSoup(ifix_page.text, BS4_PARSER)
    ifix_table_body = I.find('tbody')

    ifix = []
    df_columns = ['ticker', 'title', 'share', 'details', 'reference_date']
    parse_float = lambda x: float(x.replace(',', '.').replace('%', ''))

    for t in ifix_table_body.children:
        if type(t) == Tag:
            ifix_share = t.find('td', attrs={'class': 'fixed-column'})
            ifix_title = t.find('p')
            ifix_ticker = t.find('a')
            if all([ifix_share, ifix_title, ifix_ticker]):
                ifix_details = ifix_ticker.get('href')
                if type(ifix_details) == Tag:
                    ifix_details = _tag_to_str(ifix_details)
                ifix.append((
                    _tag_to_str(ifix_ticker), 
                    _tag_to_str(ifix_title), 
                    parse_float(_tag_to_str(ifix_share)), 
                    ifix_details, 
                    CAPTURE_DATE
                ))

    return pd.DataFrame(ifix, columns=df_columns[0:len(ifix[0])])


def _get_fii_dy_ranking_data(fii_dy_details_url):
    """
    Retrieves dividend yield ranking data for FIIs (Foreign Institutional Investors).
     Args:
        fii_dy_details_url (str): The URL of the webpage containing the FII dividend yield details.
     Returns:
        pandas.DataFrame: A DataFrame containing the extracted dividend yield ranking data.
     Purpose:
        This function provides a convenient way to retrieve and process dividend yield ranking data for FIIs from a
        given webpage URL. The extracted data can be further analyzed or used for various purposes.
    """
    dy_page = requests.get(fii_dy_details_url, headers=HEADERS)

    Y = BeautifulSoup(dy_page.text, BS4_PARSER)

    dy_columns = [
        'ticker', 'dy_current', 'p_vp', 'daily_liquidity', 
        'daily_liquidity_unit', 'net_worth', 'net_worth_unit', 
        'var_last_12_months', 'fund_type', 'segment', 'reference_date'
    ]

    dy_ranking_table = Y.find('table', id='rankigns')
    dy_rows = dy_ranking_table.findAll('tr')

    dy_data = []
    for dy_row in dy_rows[1:]:
        dy_ticker, dy_current, dy_p_vp, dy_daily_liquidity, dy_net_worth, dy_ytd_var, dy_fund_type, dy_segment = dy_row.findAll('td')

        float_from_comma_str = lambda x: float(str(x).replace(',', '~').replace('.', '').replace('~', '.')) 

        dy_ticker = _tag_to_str(dy_ticker)
        dy_current = float(_tag_to_str(dy_current))
        dy_p_vp = float(_tag_to_str(dy_p_vp))

        dy_daily_liquidity, dy_daily_liquidity_unit = _tag_to_str(dy_daily_liquidity).split(' ')
        dy_daily_liquidity = float_from_comma_str(dy_daily_liquidity)

        dy_net_worth, dy_net_worth_unit = _tag_to_str(dy_net_worth).split(' ')
        dy_net_worth = float_from_comma_str(dy_net_worth)

        dy_ytd_var = float(_tag_to_str(dy_ytd_var))

        dy_fund_type = _tag_to_str(dy_fund_type)
        dy_segment = _tag_to_str(dy_segment)

        dy_data.append((
            dy_ticker, dy_current, dy_p_vp, dy_daily_liquidity, dy_daily_liquidity_unit, 
            dy_net_worth, dy_net_worth_unit, dy_ytd_var, dy_fund_type, dy_segment, CAPTURE_DATE
        ))

    return pd.DataFrame(dy_data, columns=dy_columns)


def _get_fii_indicators(info, sleep=None):
    """
    Retrieves FII (Fundo de Investimento Imobiliário) indicators from a given URL.
     Args:
        info (tuple): A tuple containing information about FII.
            - If len(info) == 4, the tuple should contain (fii_ticker, fii_details_url, capture_date, sleep).
            - If len(info) != 4, the tuple should contain (fii_ticker, fii_details_url, capture_date).
        sleep (float, optional): The sleep time in seconds between requests. Defaults to None.
     Returns:
        dict: A dictionary containing FII indicators.
            - The dictionary contains the following keys:
                - 'PAPEL': The FII ticker.
                - 'URL': The FII details URL.
                - 'DATA_REFERÊNCIA': The capture date.
                - 'COTAÇÃO': The FII price.
                - 'DATA_COTAÇÃO': The FII price date.
                - Other FII indicators retrieved from the webpage.
     Raises:
        SystemError: If the HTTP status code is not 200.
     Notes:
        - This function uses the requests library to retrieve the webpage content.
        - It also uses BeautifulSoup for parsing the HTML content.
        - The function expects the HTML structure of the webpage to be consistent.
        - If the sleep argument is provided, the function will sleep for the specified time before returning.
     Example:
        info = ('FII_TICKER', 'https://example.com/fii_details', '2022-01-01')
        indicators = _get_fii_indicators(info, sleep=0.5)
        print(indicators)
        # Output: {'PAPEL': 'FII_TICKER', 'URL': 'https://example.com/fii_details', 'DATA_REFERÊNCIA': '2022-01-01', ...}
    """
    sleep = sleep or 0.3

    try:
        if len(info) == 4:
            fii_ticker, fii_details_url, capture_date, sleep = info
        else:
            fii_ticker, fii_details_url, capture_date = info

        fii_indicator = {'PAPEL': fii_ticker, 'URL': fii_details_url, 'DATA_REFERÊNCIA': capture_date}

        dt_page = requests.get(fii_details_url, headers=HEADERS)

        if dt_page.status_code != 200:
            raise SystemError(f'Http Error not 200: {dt_page.status_code}')

        D = BeautifulSoup(dt_page.text, BS4_PARSER)

        fii_price = _any_to_number(_tag_to_str(
            D.find('div', attrs={'class': '_card cotacao'}).find('div', attrs={'class': '_card-body'})
        ).split(' ')[-1])
        fii_indicator['COTAÇÃO'] = fii_price

        fii_price_date = datetime.strptime(
            D.find(
                'div', attrs={'class': '_card cotacao'}
            ).find(
                'div', attrs={'class': '_card-body'}
            ).find('i').attrs['data-content'].split(' ')[-1],
            '%d/%m/%Y'
        ).date()

        fii_indicator['DATA_COTAÇÃO'] = fii_price_date
        for e in D.find('div', attrs={'id': 'table-indicators'}).findAll('div', attrs={'class': 'cell'}):
            k = _tag_to_str(e.find('div', attrs={'class': 'desc'}).find('span'))
            v = (
                _tag_to_str(e.find('div', attrs={'class': 'value'}).find('span')) 
            )

            fii_indicator[k] = v

        v = _any_to_number(fii_indicator['NUMERO DE COTISTAS'])
        fii_indicator['NUMERO DE COTISTAS'] = None if v is None else int(v)

        v = _any_to_number(fii_indicator['COTAS EMITIDAS'])
        fii_indicator['COTAS EMITIDAS'] = None if v is None else int(v)
        fii_indicator['VAL. PATRIMONIAL P/ COTA'] = _any_to_number(fii_indicator['VAL. PATRIMONIAL P/ COTA'].split(' ')[-1])

        _, v2, v3 = fii_indicator['VALOR PATRIMONIAL'].split(' ')
        fii_indicator['VALOR PATRIMONIAL'] = _any_to_number(v2)
        fii_indicator['VALOR PATRIMONIAL UNIT'] = v3

        fii_indicator['ÚLTIMO RENDIMENTO'] = _any_to_number(fii_indicator['ÚLTIMO RENDIMENTO'].split(' ')[-1])

        fii_indicator['VACÂNCIA'] = _any_to_number(fii_indicator['VACÂNCIA'])

        if sleep: time.sleep(sleep)
        
        return fii_indicator
    except Exception:
        return None


def get_fii_daily_position(parallelize=True):
    """
    Retrieves daily position of FII (Fundo de Investimento Imobiliário) by querying data from various sources and storing
    them in an SQLite database in memory. The function then joins the queried data and returns a DataFrame containing the
    daily position of FII.
     Args:
        parallelize (bool, optional): A flag to determine whether to use parallel processing. Defaults to True. 
        If True and the system has more than one CPU, the function will use multiprocessing to retrieve FII indicators.
     Returns:
        pandas.DataFrame: A DataFrame containing the daily position of FII. The DataFrame contains the following
        columns: 'payment_year', 'payment_year_month', 'payment_date', 'com_date', 'ticker', 'name', 'fund_name',
        'fund_id', 'fund_type', 'segment', 'audience', 'mandate_type', 'term_type', 'management_type', 'admin_rate',
        'payment', 'price', 'price_date', 'payment_price_ratio', 'ifix_share', 'dy_current', 'p_vp', 'daily_liquidity',
        'daily_liquidity_unit', 'net_worth', 'net_worth_unit', 'var_last_12_months', 'vacancy', 'shareholders', 'shares',
        'equity_by_share', 'equity', 'equity_unit', 'last_payment', 'reference_date'.
     Raises:
        Exception: If there is an error in retrieving data from the URLs or in processing the data.
     Notes:
        - This function uses the pandas library to read and write data from/to the SQLite database.
        - It also uses the sqlite3 library to execute SQL queries.
        - The function expects the URLs to return data in a specific format.
        - If the parallelize argument is True and the system has more than one CPU, the function will use multiprocessing
            to retrieve FII indicators. Otherwise, it will retrieve the indicators sequentially.
     Example:
        df = get_fii_daily_position()
        print(df.head())
    """
    PARALLELIZE = parallelize and os.cpu_count()>1

    db = sqlite3.connect(':memory:')

    read_sql = lambda x, y={}: pd.read_sql(x, params=y, con=db)

    try:
        _get_fii_all_payment_data(FIIS_PAYMENT_URL).to_sql('fii_payment_calendar', index=False, if_exists='replace', con=db)

        _get_ifix_data(IFIX_PAGE_URL).to_sql('fii_ifix_position', con=db, index=False, if_exists='replace')

        _get_fii_dy_ranking_data(FIIS_DY_DETAILS_URL).to_sql('fii_dividend_yeld_ranking', con=db, index=False, if_exists='replace')

        cursor = db.cursor()
        cursor.execute('create index fii_payment_calendar_i01 on fii_payment_calendar (substr(payment_date, 1, 4))')
        cursor.execute('create index fii_payment_calendar_i02 on fii_payment_calendar (payment_date, ticker)')
        cursor.execute('create index fii_ifix_position_i01 on fii_ifix_position (ticker)')
        cursor.execute('create index fii_dividend_yeld_ranking_i01 on fii_dividend_yeld_ranking (ticker)')

        fii_info = read_sql('''
        select distinct ticker, details
            from fii_payment_calendar
        ''')
        fii_info['capture_date'] = CAPTURE_DATE
        fii_info = tuple(fii_info.to_records(index=False))

        if PARALLELIZE:
            with Pool(os.cpu_count()) as p:
                data = p.map(_get_fii_indicators, fii_info)
        else:
            data = []
            for info in fii_info:
                data.append(_get_fii_indicators(info))

        fii_indicators_df = pd.DataFrame.from_dict(
            [d for d in data if d]
        )

        fii_indicators_df.columns = [
            'ticker', 'url', 'ref_date', 'price', 'price_date',
            'fund_name', 'fund_id', 'audience', 'mandate_type', 'segment',
            'fund_type', 'term_type', 'management_type',
            'admin_rate', 'vacancy', 'shareholders',
            'shares', 'equity_by_share', 'equity',
            'last_payment', 'equity_unit'
        ]

        fii_indicators_df[[
            'ticker', 'url', 'ref_date', 'price', 'price_date',
            'fund_name', 'fund_id', 'audience', 'mandate_type', 'segment',
            'fund_type', 'term_type', 'management_type',
            'admin_rate', 'vacancy', 'shareholders',
            'shares', 'equity_by_share', 'equity', 'equity_unit',
            'last_payment'
        ]].to_sql('fii_indicators', con=db, index=False, if_exists='replace')

        cursor.execute('create index fii_indicators_i01 on fii_payment_calendar (ticker)')

        return read_sql(f"""
            select 
                substr(p.payment_date, 1, 4)       as payment_year,
                substr(p.payment_date, 1, 7)       as payment_year_month,
                substr(p.payment_date, 1, 10)      as payment_date,
                substr(p.com_date, 1, 10)          as com_date,
                p.ticker,
                coalesce(f.title, p.name)          as name,
                i.fund_name, 
                i.fund_id, 

                coalesce(r.fund_type, i.fund_type) as fund_type,
                coalesce(r.segment, i.segment)     as segment,
                i.audience, 
                i.mandate_type, 
                i.term_type, 
                i.management_type,
                i.admin_rate,

                p.payment,
                i.price, 
                i.price_date,
                case 
                    when i.price is not null 
                    then p.payment / i.price 
                    else null 
                end payment_price_ratio, 

                f.share                            as ifix_share,
                r.dy_current,
                r.p_vp,
                r.daily_liquidity,
                r.daily_liquidity_unit,
                r.net_worth,
                r.net_worth_unit,
                r.var_last_12_months,

                i.vacancy, 
                i.shareholders,
                i.shares, 
                i.equity_by_share, 
                i.equity, 
                i.equity_unit,
                i.last_payment,

                p.reference_date
            from fii_payment_calendar as p
            left join fii_ifix_position as f 
                using (ticker)
            left join fii_dividend_yeld_ranking as r 
                using (ticker)
            left join fii_indicators as i
                using (ticker)
            where substr(p.payment_date, 1, 4) <> '9999'
            order by payment_date, ticker
        """)
    finally:
        db.close()
