'''
Data Providers: CVM Update Package
Provides updating data modules, functions and classes for CVM (Comissão de Valores Imobiliários) provider.
'''

import os
import io
import csv
import re
import requests
import bs4
import sqlite3
import pandas as pd

from bs4 import BeautifulSoup
from time import sleep
from zipfile import ZipFile
from datetime import datetime
from dateutil.parser import parse
from urllib import request
from multiprocessing import Pool
from typing import Union, Dict, Optional

import fbpyutils_finance as FI
from fbpyutils_finance.cvm.converters import *
from fbpyutils import string as SU, file as FU

from fbpyutils.debug import debug, debug_info


URL_IF_REGISTER = "http://dados.cvm.gov.br/dados/FI/CAD/DADOS"
URL_IF_REGISTER_HIST = "http://dados.cvm.gov.br/dados/FI/CAD/DADOS/HIST"
URL_IF_DAILY = "http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS"
URL_IF_DAILY_HIST = "http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST"

SOURCE_ENCODING, TARGET_ENCODING = 'iso-8859-1', 'utf-8'

HEADERS_FILE = os.path.sep.join([FI.APP_FOLDER, 'cvm', 'data', 'if_headers_v4.xlsx'])
HEADER_MAPPINGS_FILE = os.path.sep.join([FI.APP_FOLDER, 'cvm', 'data', 'if_header_mappings.xlsx'])

if not os.path.exists(HEADERS_FILE):
    raise FileNotFoundError('CVM Headers File not found.')

if not os.path.exists(HEADER_MAPPINGS_FILE):
    raise FileNotFoundError('CVM Headers Mappings File not found.')

HEADERS = pd.read_excel(HEADERS_FILE, sheet_name='IF_HEADERS')
HEADER_MAPPINGS = pd.read_excel(HEADER_MAPPINGS_FILE, sheet_name='IF_HEADERS')


def _get_value_by_index_if_exists(x, y, z=None):
    """
    Returns the value at index y in list x if it exists, otherwise returns z or None.
     Args:
        x (list): The input list.
        y (int): The index to retrieve the value from.
        z (optional): The default value to return if the index is out of range. Defaults to None.
     Returns:
        The value at index y in list x if it exists, otherwise returns z or None.
     Overview of General Functionality:
        The _get_value_by_index_if_exists function allows retrieving a value from a list at a specified index, with the 
        flexibility to provide a default value if the index is out of range or the list is empty.
    """
    return x[y] if len(x) > y else z or None


def _make_number_type(x, y=int):
    """
    Converts the input x to a specified number type y.
     Args:
        x (str): The input value to be converted.
        y (type, optional): The desired number type to convert to. Defaults to int.
     Returns:
        The converted value of x to the specified number type y, or None if x is None or '-'.
     Overview of General Functionality:
        The _make_number_type function is responsible for converting a string representation of a number to a specified number 
        type. It handles cases where x is None or '-', returning None in those cases. The default number type is int, but it can 
        be customized by providing a different number type y.
    """
    return None if x is None or x == '-' else y(re.sub(r'[a-zA-Z]', '', x))


def _timelapse(x):
    """
    Calculates the time elapsed in minutes between the current datetime and a given datetime.
     Args:
        x (datetime): The datetime to calculate the time elapsed from.
     Returns:
        float: The time elapsed in minutes, rounded to 4 decimal places.
     Overview of General Functionality:
        The _timelapse function calculates the time elapsed in minutes between the current datetime and a given datetime. It 
        provides a convenient way to measure the time difference in minutes.
    """
    return round((datetime.now() - x).seconds / 60, 4)


def _replace_all(x: str, old: str, new: str) -> str:
    """
    Replace all occurrences of `old` with `new` in `x`.

    Args:
    - x (str): The input string.
    - old (str): The string to be replaced.
    - new (str): The replacement string.

    Returns:
    - str: The input string with all occurrences of `old` replaced by `new`.

    Example:
    ```
    x = 'hello, world'
    old = 'o'
    new = '0'
    print(_replace_all(x, old, new))
    # Output: 'hell0, w0rld'
    ```
    """
    while old in x:
        x = x.replace(old, new)
    return x


def _make_datetime(x: str, y: str) -> Union[datetime, None]:
    """
    Convert a string representation of a date and time into a datetime object.

    Args:
    - x (str): The string representation of the date.
    - y (str): The string representation of the time.

    Returns:
    - datetime: The resulting datetime object, or None if either `x` or `y` are None.

    Example:
    ```
    x = '29-Jan-2023'
    y = '08:30'
    print(_make_datetime(x, y))
    # Output: datetime.datetime(2023, 1, 29, 8, 30)
    ```
    """
    sep = ' '
    if not all([x, y]):
        return None
    else:
        dt = sep.join([x, y])
        return datetime.strptime(dt, "%d-%b-%Y %H:%M")


def _get_url_paths(url: str, params: Optional[Dict] = {}) -> pd.DataFrame:
    """
    Get the URL paths of the given URL with the given parameters.

    Args:
    - url: URL to get the paths from
    - params: query parameters to include in the request, if any (default: {})

    Returns:
    - DataFrame of the extracted URL paths information, with columns:
        - 'sequence': index of the path in the extracted list
        - 'href': the URL of the path
        - 'name': name of the path
        - 'last_modified': last modification date of the path
        - 'size': size of the path in bytes
    
    """
    response = requests.get(url, params=params)
    if response.ok:
        response_text = response.text
    else:
        return response.raise_for_status()
    soup = BeautifulSoup(response_text, 'html.parser')
    pre = soup.find_all('pre')

    pre = pre[0] if len(pre) > 0 else None

    sep = pre.text[3:5]

    hrefs = [a.get('href') for a in [p for p in pre] if type(a) if type(a) == bs4.element.Tag]

    contents = [_replace_all(p, '  ', ' ').split(' ') for p in pre.text.split(sep)]

    directory = set()

    for i, href in enumerate(hrefs):
        content = contents[i]
        name = _get_value_by_index_if_exists(content, 0, '').split('.')[0]
        last_modified = _make_datetime(
            _get_value_by_index_if_exists(content, 1), 
            _get_value_by_index_if_exists(content, 2)
        )
        size = _make_number_type(_get_value_by_index_if_exists(content, 3))

        directory.add((
            i,
            href,
            name,
            last_modified,
            size
        ))

    headers = ['sequence', 'href', 'name', 'last_modified', 'size']
    directory = pd.DataFrame(directory, columns=headers).sort_values(by='sequence', ascending=True)

    return directory


# @debug
def _build_target_file_name(metadata, target_folder, index=None, file=None):
    """
    Builds the target file name based on the given metadata, target folder, index, and file.
    Args:
        metadata (dict): The metadata dictionary containing information about the file.
        target_folder (str): The path to the target folder where the file will be saved.
        index (int, optional): The index number to be included in the file name. Defaults to None.
        file (str, optional): The file extension to be included in the file name. Defaults to None.
    Returns:
        str: The full path to the target file name.
    Overview of General Functionality:
        The _build_target_file_name function is responsible for constructing the target file name based on the given metadata, 
        target folder path, index number, and file extension. It provides a flexible way to generate file names for saving files 
        in a specific folder.
    """
    preffix = metadata['href'].split('.')[0]

    if file:
        index = str(int('0' if index is None else index)).zfill(4)
        target_file_name = '.'.join([preffix, index, file])
    else:
        target_file_name = metadata['href']

    target_file_name = '.'.join([metadata['kind'].lower(), target_file_name])

    return os.path.sep.join([target_folder, target_file_name])


# @debug
def _write_target_file(data, metadata, target_folder, index=None, file=None, encoding=TARGET_ENCODING):
    """
    Writes the provided data to a target file with the specified metadata, target folder, index, file, and encoding.
    Args:
        data (str): The data to be written to the target file.
        metadata (dict): The metadata dictionary containing information about the file.
        target_folder (str): The path to the target folder where the file will be saved.
        index (int, optional): The index number to be included in the file name. Defaults to None.
        file (str, optional): The file extension to be included in the file name. Defaults to None.
        encoding (str, optional): The encoding to be used for writing the data to the file. Defaults to TARGET_ENCODING.
    Returns:
        str: The path to the target file where the data was written.
    Overview of General Functionality:
        The _write_target_file function is responsible for writing the provided data to a target file with the specified metadata, 
        target folder, index, file, and encoding. It handles the file writing process and returns the path to the written file.
    """
    target_file = _build_target_file_name(metadata, target_folder, index, file)

    with open(target_file, 'wb') as f:
        f.write(data.encode(encoding))
        f.close()
    
    return target_file


# @debug
def _update_cvm_history_file(if_metadata):
    """
    Updates the history file for the given metadata.
    Args:
        if_metadata (dict): The metadata for the file to update.
        history_folder (str, optional): The folder to store the history file. If not provided, the default folder will be used.
    Returns:
        list: A list containing the result of the update operation.
    Overview of General Functionality:
        The _update_cvm_history_file function updates the history file for the given metadata. It checks if the file needs to be updated, 
        downloads the file if necessary, determines the file type, and writes the file to the history folder. The result of the update 
        operation is returned as a list of status, metadata, and message.
    """
    result = []
    if is_nan_or_empty(if_metadata['last_download']) \
        or (if_metadata['last_modified'] > if_metadata['last_download']):

        response = request.urlopen(if_metadata['url'])

        data = response.read()

        download_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        mime_type = FU.magic.from_buffer(data)
        mime_type = mime_type.split(';')[0]

        text_mime_types = ('Non-ISO extended-ASCII text', 'ISO-8859 text', )
        zip_mime_types = ('Zip archive data, at least v2.0 to extract', )

        if any([a for a in zip_mime_types if a in mime_type]):
            zip_file = ZipFile(io.BytesIO(data))
            for k, v in enumerate(zip_file.namelist()):
                response_data = zip_file.open(v).read().decode(SOURCE_ENCODING)
                r = _write_target_file(
                    response_data, if_metadata, if_metadata['history_folder'], index=k, file=v, encoding=TARGET_ENCODING)
                if_metadata['last_download'] = download_time
                if_metadata['history_file'] = r.split(os.path.sep)[-1]
                result.append(['SUCCESS',if_metadata,f'{r} written from {if_metadata["url"]}'])
        elif any([a for a in text_mime_types if a in mime_type]):
            response_data = data.decode(SOURCE_ENCODING)

            r = _write_target_file(response_data, if_metadata, if_metadata['history_folder'], encoding=TARGET_ENCODING)
            if_metadata['last_download'] = download_time
            if_metadata['history_file'] = r.split(os.path.sep)[-1]

            result.append(['SUCCESS',if_metadata, f'{r} written from {if_metadata["url"]}'])
        else:
            result.append(['ERROR',if_metadata,'Unknown mime type:{} for url:{}'.format(mime_type, if_metadata['url'])])
    else:
        result.append(['SKIP','Already updated data for url:{}'.format(if_metadata['url'])])
    
    return result


# @debug
def _get_remote_files_list(kind: str, current_url: str, history_url: str) -> pd.DataFrame:
    """
    Get a DataFrame containing information of files in remote locations.

    Args:
    - kind (str): Kind of files to retrieve.
    - current_url (str): URL string of the current location.
    - history_url (str): URL string of the history location.

    Returns:
    - pd.DataFrame: DataFrame containing information of the remote files, including `kind`, `url`, `history`
    and other information extracted from the URL paths.
    """
    current_dir = _get_url_paths(current_url)
    current_dir['history'] = False
    current_dir['url'] = current_url

    history_dir = _get_url_paths(history_url)
    history_dir['history'] = True
    history_dir['url'] = history_url

    files_dir = pd.concat([
        current_dir[~pd.isnull(current_dir['size'])],
        history_dir[~pd.isnull(history_dir['size'])]
    ]).copy()

    files_dir['kind'] = kind

    return files_dir


# @debug
def _get_expression_and_converters(mappings):
    """
    Extracts expressions and converters from the given mappings.
    Args:
        mappings (list): A list of dictionaries containing mapping information.
    Returns:
        tuple: A tuple containing two elements:
            - expressions (list): A list of expressions extracted from the mappings.
            - converters (dict): A dictionary of converters extracted from the mappings.
    Overview of General Functionality:
        The _get_expression_and_converters function is responsible for extracting expressions and converters from the given 
        mappings. It processes the mappings to generate the necessary expressions and converters for data transformation.
    """
    expressions, converters = [], {}

    for m in mappings:
        expression = 'NULL'
        if not is_nan_or_empty(m['Source_Field']):
            expression = m['Source_Field']
            if not is_nan_or_empty(m['Transformation1']):
                expression = m['Transformation1'].replace('$X', expression)
                if not is_nan_or_empty(m['Transformation2']):
                    expression = m['Transformation2'].replace('$X', expression)
                    if not is_nan_or_empty(m['Transformation3']):
                        expression = m['Transformation3'].replace('$X', expression)

        if not is_nan_or_empty(m['Converter']):
            converters[m['Target_Field']] = eval(m['Converter'].replace('_as_', 'as_'))
        else:
            converters[m['Target_Field']] = lambda x: None

        expressions.append(f"{expression} AS {m['Target_Field'].lower()}")

    return expressions, converters


# @debug
def _apply_converters(data, converters):
    """
    Applies converters to the specified columns in the given DataFrame.
    Args:
        data (pandas.DataFrame): The DataFrame to apply converters to.
        converters (dict): A dictionary of converters to apply.
    Returns:
        pandas.DataFrame: The modified DataFrame with converters applied.
    Overview of General Functionality:
        The _apply_converters function is responsible for applying converters to the specified columns in the given DataFrame. 
        It iterates over the converters dictionary and applies the corresponding converter function to each column, modifying 
        the DataFrame in-place.
    """
    try:
        for k, v in converters.items():
            if k in data.columns:
                data[k] = data[k].apply(v)
        return data.where(pd.notnull(data), None)
    except Exception as E:
        info = debug_info(E)
        raise ValueError(f"Conversion error: {E} ({info}) on {k}:{v}")


# @debug
def _apply_expressions(data, expressions):
    """
    Applies expressions to the specified DataFrame using an in-memory SQLite database.
    Args:
        data (pandas.DataFrame): The DataFrame to apply expressions to.
        expressions (list): A list of expressions to apply.
    Returns:
        pandas.DataFrame: The resulting DataFrame after applying the expressions.
    Overview of General Functionality:
        The _apply_expressions function applies the specified expressions to the given DataFrame using an in-memory SQLite 
        database. It stores the DataFrame in the database, executes an SQL query to select the specified expressions, and 
        returns the resulting DataFrame.
    """
    STAGE=sqlite3.connect(':memory:')
    try:
        data.to_sql('if_data', con=STAGE, if_exists='replace')

        return pd.read_sql(f'SELECT {", ".join(expressions)} FROM if_data', con=STAGE)
    finally:
        STAGE = None


# @debug
def _get_cvm_file_metadata(cvm_file):
    """
    Analyzes a given cvm_file and retrieves its metadata.
    Parameters:
    - cvm_file: A string representing the path to the cvm_file.
    Returns:
    - kind: A string representing the kind of the cvm_file.
    - sub_kind: A string representing the sub-kind of the cvm_file.
    - line: A string representing the first line of the cvm_file.
    - hash_value: A string representing the hash value of the metadata.
    Code Overview:
    The code reads the first line of a cvm_file and extracts its metadata. It determines the kind
    and sub-kind of the file based on its name. The resulting metadata is returned along with a hash
    value computed from the metadata.
    """
    with open(cvm_file, 'r') as f:
        line = f.readline()
        f.close()

    file_name_parts = cvm_file.split(os.path.sep)[-1].split('.')
    kind = file_name_parts[0].upper()
    metadata_file = file_name_parts[-2]
    sub_kind = 'CAD_FI' if 'cad_fi' == metadata_file.lower() or metadata_file.lower().startswith('inf_cadastral_fi') \
        else 'DIARIO_FI' if metadata_file.lower().startswith('inf_diario_fi') else metadata_file.upper()
    line = line.split('\n')[0]

    return kind, sub_kind, line, SU.hash_string(';'.join([kind, sub_kind, line]))


# @debug
def _get_cvm_updated_headers(cvm_files: list) -> list[dict]:
    """
    This method retrieves updated headers from given cvm files and returns a list of dictionaries where each dictionary 
    represents a header mapping.
     Parameters:
    cvm_files (list): A list of cvm files from which headers need to be retrieved.
     Returns:
    list[dict]: A list of dictionaries where each dictionary represents a header mapping.
     Raises:
    FileNotFoundError: If the header mappings file does not exist.
    ValueError: If cvm_files is None or not a list, or if both source files and existing mappings are empty.
    Exception: If there is any error in getting cvm file metadata or in any step of the function.
     Example Usage:
    cvm_files = ["file1", "file2", "file3"]
    updated_headers = _get_cvm_updated_headers(cvm_files)
    """
    step = 'SETTING UP COMPONENTS'
    STAGE=sqlite3.connect(':memory:')
    try:
        step = 'READING HEADERS MAPPINGS INFO'
        if not os.path.exists(HEADER_MAPPINGS_FILE):
            raise FileNotFoundError(f"Header Mappings not found.")

        _ = pd.read_excel(
            HEADER_MAPPINGS_FILE, sheet_name='IF_HEADERS'
        ).to_sql('cvm_if_headers_stg', con=STAGE, index=False, if_exists='replace')

        header_mappings = {}

        for header in pd.read_sql("""
            select distinct "Header"
            from cvm_if_headers_stg
        """, con=STAGE).to_dict('records'):
            mappings = pd.read_sql(f"""
            select  "Order", 
                    "Target_Field", 
                    "Source_Field", 
                    "Transformation1", 
                    "Transformation2",
                    "Transformation3", 
                    "Converter"
                from cvm_if_headers_stg 
               where "Header" = '{header['Header']}'
               order by "Order"
            """, con=STAGE).to_dict('records')
            
            header_mappings[header['Header']] = mappings

        step = 'READING IF HISTORY FILES'
        if cvm_files is not None and type(cvm_files) == list:
            if_source_files = cvm_files
        else:
            raise ValueError("CVM files list empty or invalid.")

        if_source_headers = set()

        for if_register_file in if_source_files:
            try:
                kind, sub_kind, header, header_hash = _get_cvm_file_metadata(if_register_file)
            except Exception as e:
                print(f'Ouch! {e} on {if_register_file}')
                raise e
            if_source_headers.add((kind, sub_kind, header, header_hash))

        _ = pd.DataFrame(
            if_source_headers, columns=['Kind', 'Sub_Kind', 'Header', 'Hash']
        ).to_sql('cvm_if_source_headers_stg', con=STAGE, index=False, if_exists='replace')

        step = 'READING CURRENT HEADERS INFO'
        if os.path.exists(HEADERS_FILE):
            mappings = pd.read_excel(HEADERS_FILE, sheet_name='IF_HEADERS').to_dict('records')
        else:
            mappings = []

        existing_mappings = set([m.get('Hash') for m in mappings])

        if len(if_source_files) == 0 and len(existing_mappings) == 0:
            raise ValueError("Mappend Headers and/or History Files Not Found.")

        step = 'COMPUTING NEW HEADERS INFO'
        for header_group in pd.read_sql("""
            select distinct "Kind", "Sub_Kind"
            from cvm_if_source_headers_stg
        """, con=STAGE).to_dict('records'):
            kind, sub_kind = header_group['Kind'], header_group['Sub_Kind']
            header_mapping = header_mappings[kind]

            for source_header in pd.read_sql(f"""
                select * 
                from cvm_if_source_headers_stg
                where "Kind" = '{kind}' and "Sub_Kind" = '{sub_kind}'
            """, con=STAGE).to_dict('records'):
                header = source_header['Header']
                header_hash = source_header['Hash']

                if header_hash not in existing_mappings:
                    fields = header.split(';')
                    for m in header_mapping[:]:
                        found = m['Source_Field'] in fields
                        mappings.append({
                            'Kind': kind,
                            'Sub_Kind': sub_kind,
                            'Header': header,
                            'Hash': header_hash,
                            'Order': int(m['Order']),
                            'Target_Field': m['Target_Field'],
                            'Source_Field': m['Source_Field'] if found else None,
                            'Transformation1': m['Transformation1'] if found else None,
                            'Transformation2': m['Transformation2'] if found else None,
                            'Transformation3': m['Transformation3'] if found else None,
                            'Converter': m['Converter'] if found else None,
                            'Is_New': True
                        })

                        if sub_kind in ['CAD_FI', 'DIARIO_FI']:
                            max_order = max([
                                h['Order'] for h in header_mapping if h['Source_Field'] is not None])
                            source_fields = [
                                h['Source_Field'] for h in header_mapping if h['Source_Field'] is not None]
                            mapped_fields = [
                                m['Source_Field'] for m in mappings if m['Target_Field'] is not None] + [
                                    m['Source_Field'] for m in mappings if m['Transformation1'] is not None] + [
                                        m['Source_Field'] for m in mappings if m['Transformation2'] is not None] + [
                                            m['Source_Field'] for m in mappings if m['Transformation3'] is not None] 
                            for f in fields:
                                if not f in source_fields and not f in mapped_fields:
                                    max_order += 1
                                    mappings.append({
                                        'Kind': kind,
                                        'Sub_Kind': sub_kind,
                                        'Header': header,
                                        'Hash': header_hash,
                                        'Order': max_order,
                                        'Target_Field': None,
                                        'Source_Field': f,
                                        'Transformation1': None,
                                        'Transformation2': None,
                                        'Transformation3': None,
                                        'Converter': None,
                                        'Is_New': True
                                    })

        return mappings
    except Exception as E:
        info = debug_info(E)
        raise ValueError('Fail to GET CHANGED HEADERS IN CVM FILES on step {}: {}: ({})'.format(step, E, info))
    finally:
        STAGE = None


# @debug
def _check_cvm_headers_changed(cvm_files) -> bool:
    """
    Function: _check_cvm_headers_changed
    Description:
    This function checks if the headers of CVM files have been changed by comparing the hash of the current headers with the hash of the updated headers.
    It reads the current headers from an Excel file and stores them in a dictionary. It then calls the function _get_cvm_updated_headers to get the updated headers
    and compares the hashes of the new headers with the hashes of the existing headers. If there are any changes in the headers, the function returns True,
    otherwise it returns False.
     Parameters:
    - cvm_files: A list of CVM files to check for header changes.
     Returns:
    - bool: True if there are changes in the headers, False otherwise.
     Exceptions:
    - ValueError: If the function fails to get changed headers in CVM files.
     Example:
    cvm_files = ['file1.cvm', 'file2.cvm']
    headers_changed = _check_cvm_headers_changed(cvm_files)
    if headers_changed:
        print('Headers have been changed.')
    else:
        print('Headers have not been changed.')
    """
    step = None
    try:
        step = 'READING CURRENT HEADERS INFO'
        if os.path.exists(HEADERS_FILE):
            mappings = pd.read_excel(HEADERS_FILE, sheet_name='IF_HEADERS').to_dict('records')
        else:
            mappings = []

        existing_mappings = set([m.get('Hash') for m in mappings])
    
        new_mappings = _get_cvm_updated_headers(cvm_files)
        
        return set(m['Hash'] for m in new_mappings) - existing_mappings
    except Exception as E:
        info = debug_info(E)
        raise ValueError('Fail to GET CHANGED HEADERS IN CVM FILES on step {}: {}: ({})'.format(step, E, info))


# @debug
def _write_cvm_headers_mappings(mappings, file_path) -> bool:
    """
    Writes the mappings of headers to a file.
     Parameters:
    mappings (dict): A dictionary containing the mappings of headers.
    file_path (str): The path to the file where the mappings will be written.
     Returns:
    bool: True if the mappings were successfully written, False otherwise.
     Functionality Description:
    1. Sets up the components for writing the mappings.
    2. Writes the new headers mappings information to a temporary SQLite database.
    3. Reads the mapped headers from the temporary database.
    4. Writes the mapped headers to an Excel file.
    5. Returns True if the mappings were successfully written, False otherwise.
     Overview:
    This function takes a dictionary of mappings and writes them to a specified file location.
    It sets up the necessary components, writes the mappings to a temporary database, retrieves the mapped
    headers, and finally writes them to an Excel file. The function returns True if the mappings were 
    successfully written, and False otherwise.
    """
    step = 'SETTING UP COMPONENTS'
    STAGE=sqlite3.connect(':memory:')
    try:
        step = 'WRITING NEW HEADERS MAPPINGS INFO'
        if mappings:
            _ = pd.DataFrame.from_dict(mappings).to_sql('cvm_if_headers_final_stg', STAGE, index=False, if_exists='replace')

            if_headers_final = pd.read_sql("""
                select distinct 
                    "Hash",
                    "Kind",
                    "Sub_Kind",
                    "Order",
                    "Target_Field",
                    "Source_Field",
                    "Transformation1",
                    "Transformation2",
                    "Transformation3",
                    "Converter",
                    "Is_New"
                from cvm_if_headers_final_stg
                order by "Hash",
                        "Order"
            """, con=STAGE)
        
            if_headers_final.to_excel(
                file_path, sheet_name='IF_HEADERS', index=False, freeze_panes=(1, 0), header=True)

            return True
        else:
            return False
    except Exception as E:
        info = debug_info(E)
        raise ValueError('Fail TO WRITE NEW HEADERS on step {}: {}: ({})'.format(step, E, info))
    finally:
        STAGE = None


# @debug
def _read_cvm_history_file(source_file, apply_converters=True, check_header=False):
    """
    Reads and processes CVM history data from a source file.
    Parameters:
    - source_file (str): The path to the source file containing CVM history data.
    - apply_converters (bool): Flag indicating whether to apply data type conversions. Default is True.
    - check_header (bool): Flag indicating whether to check if the file header has changed. Default is False.
    Returns:
    - kind (str): The kind of CVM data.
    - sub_kind (str): The sub-kind of CVM data.
    - cvm_if_data (pandas.DataFrame): The processed CVM data.
    - partition_cols (list): List of column names used for partitioning the data.
    Raises:
    - ValueError: If the file header has changed or if header hash is not found.
    - Exception: If the headers mappings file is not found.
    - ValueError: If an invalid sub-kind or kind is encountered.
    - ValueError: If no data expressions and converters were found.
    Overview:
    This function reads and processes CVM history data from a source file. It performs the following steps:
    1. Checks the file header if specified.
    2. Retrieves data information from the source file.
    3. Reads the current headers mappings from a file.
    4. Applies data expressions to the source data.
    5. Applies data type conversions if specified.
    6. Computes period information based on the kind and sub-kind of the data.
    7. Selects the data to return based on partitioning columns.
    Note:
    - The source file should be in CSV format with ';' delimiter and encoded in the TARGET_ENCODING.
    - The headers mappings file should be an Excel file with a sheet named 'IF_HEADERS'.
    """
    try:
        step = 'CHECK FILE HEADER'
        if check_header:
            if _check_cvm_headers_changed([source_file]):
                raise ValueError('Header changed!!')

        step = 'DATA INFO FROM SOURCE FILE'
        kind, sub_kind, _, header_hash = _get_cvm_file_metadata(source_file)

        if not header_hash:
            raise ValueError("Header hash not found!")

        step = 'READING CURRENT HEADERS INFO'
        header_mappings = HEADERS
        mappings = header_mappings[header_mappings.Hash == header_hash].to_dict('records')
        expressions, data_converters = _get_expression_and_converters(mappings)

        if not all([mappings, expressions]):
            raise ValueError('No expressions and mappings found. Does headers changed?')

        step = 'READING DATA FROM SOURCE FILE'
        if_data = pd.read_csv(source_file, sep=';', encoding=TARGET_ENCODING, dtype=str, quoting=csv.QUOTE_NONE)
        if_data.columns = [c.lower() for c in if_data.columns]

        step = 'APPLYING DATA EXPRESSIONS'
        cvm_if_data = _apply_expressions(if_data, expressions=expressions)

        if apply_converters:
            if not data_converters:
                raise ValueError('No converters found. Does headers changed?')
            step = 'APPLYING DATA TYPES CONVERSIONS'
            cvm_if_data = _apply_converters(cvm_if_data.copy(), data_converters)

        cvm_if_data_cols = list(cvm_if_data.columns)
        cvm_if_data['kind'] = kind
        cvm_if_data['sub_kind'] = sub_kind

        step = 'COMPUTING PERIOD INFO'
        if kind == 'IF_POSITION':
            cvm_if_data['year'] = cvm_if_data['position_date'].apply(lambda x: datetime.fromisoformat(str(x)).strftime('%Y'))
            cvm_if_data['period'] = cvm_if_data['position_date'].apply(lambda x: datetime.fromisoformat(str(x)).strftime('%Y-%m'))
        elif kind == 'IF_REGISTER':
            if sub_kind == 'CAD_FI':
                file_name = source_file.split(os.path.sep)[-1]
                date_part = file_name.split('.')[-2].split('_')[-1]

                if date_part == 'fi':
                    date_part = datetime.now().strftime("%Y-%m-%d")

                cvm_if_data['year'] = pd.to_datetime(date_part, format='%Y-%m-%d').strftime("%Y")
                cvm_if_data['period'] = pd.to_datetime(date_part, format='%Y-%m-%d').strftime("%Y-%m")
                cvm_if_data['period_date'] = pd.to_datetime(date_part, format='%Y-%m-%d').strftime('%Y-%m-%d')
            elif sub_kind.startswith('CAD_FI_HIST'):
                cvm_if_data['year'] = cvm_if_data['start_date'].apply(lambda x: datetime.fromisoformat(str(x)).strftime('%Y'))
                cvm_if_data['period'] = cvm_if_data['start_date'].apply(lambda x: datetime.fromisoformat(str(x)).strftime('%Y-%m'))
            else:
                raise ValueError(f'Invalid Sub Kind: {sub_kind} on {source_file}')
        else:
            raise ValueError(f'Invalid Kind: {kind} on {source_file}')

        step = 'SELECT DATA TO RETURN'
        partition_cols = ['kind', 'sub_kind']
        for c in ['year', 'period', 'period_date']:
            if c in cvm_if_data.columns:
                partition_cols += [c]

        return kind, sub_kind, cvm_if_data[partition_cols + cvm_if_data_cols], partition_cols
    except Exception as E:
        info = debug_info(E)
        raise ValueError('Fail to get CVM IF HISTORY Data on step {}: {} ({})'.format(step, E, info))


# @debug
# def _process_cvm_history_file(cvm_file_info):
#     """
#     Process the CVM history file.
#     Args:
#         cvm_file_info (tuple): A tuple containing three elements - name (str), cvm_file (str), and update_time (datetime).
#     Returns:
#         list: A list of lists, where each inner list contains the following elements:
#             - name (str): The name of the file.
#             - kind (str): The kind of file.
#             - sub_kind (str): The sub-kind of file.
#             - cvm_file (str): The file path.
#             - update_time (datetime): The update time.
#             - len(cvm_if_data) (int): The length of cvm_if_data.
#             - partition_cols (str): The partition columns.
#             - _timelapse(start_time) (str): The time taken for the process.
#             - step (str): The current step in the process.
#             - 'SUCCESS' or 'ERROR' (str): Indicates whether the process was successful or encountered an error.
#             - Error message (str): Provides additional information in case of an error.
#     Overview:
#     This function processes a CVM history file. It initializes parameters and variables, reads the metadata of the CVM file,
#     connects to an in-memory SQLite database, writes the CVM data to a staging table in the database, appends the result to the result list,
#     closes the database connection, and finally returns the result list.
#     Note: If any exception occurs during the process, the code handles it by appending an error message to the result list.
#     """
#     result = []
#     start_time = datetime.now()

#     step = 'SETTING UP COMPONENTS'
#     STAGE=sqlite3.connect(':memory:')
#     try:
#         step = 'INITIALIZING PARAMETERS'
#         name, cvm_file, update_time = cvm_file_info

#         step = 'READING CVM FILE METADATA'
#         kind, sub_kind, cvm_if_data, partition_cols = _read_cvm_history_file(
#             source_file=cvm_file, 
#             apply_converters=True,
#             check_header=False
#         )
#         try:
#             target_table = f'cvm_{kind}_{sub_kind}_history_stg'.lower()

#             cvm_if_data['source'] = '.'.join(cvm_file.split(os.path.sep)[-1].split('.')[0:-1])
#             cvm_if_data['timestamp'] = update_time.strftime('%Y-%m-%d %H:%M:%S')

#             step = 'WRITING CVM DATA STAGE'
#             cvm_if_data.to_sql(target_table, index=False, if_exists='append', con=STAGE)

#             result.append([
#                 name, kind, sub_kind, cvm_file, update_time, len(cvm_if_data), partition_cols, 
#                 _timelapse(start_time), step, 'SUCCESS', None
#             ])
#         except Exception as E:
#             result.append([
#                 name, kind, sub_kind, cvm_file, update_time, len(cvm_if_data), partition_cols, 
#                 _timelapse(start_time), step, 'ERROR', f'ERROR: {E}'
#             ])
#     except Exception as E:
#         info = debug_info(E)
#         if len(cvm_file_info) == 3:
#             name, cvm_file, update_time = cvm_file_info
#         else:
#             name, cvm_file, update_time = None, None, None
#         result.append([
#             name, None, None, cvm_file, update_time, None, None, 
#             _timelapse(start_time), step, 'ERROR', f'ERROR {E} ({info}): WITH info={cvm_file_info}'
#         ])
#     finally:
#         STAGE = None

#     return result

# @debug
# def update_cvm_history_data(parallelize=True):
#     """
#     Updates the history data of Comissão de Valores Mobiliários (CVM) files.
#     Parameters:
#     - parallelize (bool, optional): A boolean flag to indicate if the function should be parallelized. Defaults to True.
#     - history_folder (str, optional): The path to the folder that contains the history data. Defaults to None.
#     Returns:
#     - results (list): A list of dictionaries containing the result data of the CVM files processing.
#     Code Overview:
#     The code updates the history data of CVM files. It checks the validity of the history_folder and the possibility
#     of parallel execution. It then connects to an in-memory SQLite database and tries to execute several steps to
#     update the history data. If an error occurs during the execution of the steps, it raises a ValueError with an
#     appropriate error message. Finally, it closes the connection to the SQLite database.
#     """
#     PARALLELIZE = parallelize and os.cpu_count()>1

#     # disabling parallel processing due to errors with sqlite3
#     PARALLELIZE = False

#     step = 'SETUP COMPONENTS'
#     STAGE = sqlite3.connect(':memory:')
#     try:
#         step = 'LOAD CATALOG UPDATES'
#         catalog_updates = pd.read_sql(f'''
#             select * 
#             from {self.CATALOG_JOURNAL} 
#             where active 
#             and last_updated is null 
#                 or last_download > last_updated
#             order by kind, name
#         ''', con=self.CATALOG).to_dict(orient='records')

#         results = []
#         if len(catalog_updates) > 0:
#             step = 'SELECTING FILES TO UPDATE'
#             cvm_files = []
#             for name, mask in [
#                 (u['name'], f"{u['kind'].lower()}.{u['name'].split('.')[0]}.*") for u in catalog_updates
#             ]:
#                 for cvm_file in FU.find(self.HISTORY_FOLDER, mask):
#                     cvm_files += [[name, cvm_file]]

#             if len(cvm_files) == 0:
#                 return []

#             step = 'CHECKING HEADERS'
#             if self._check_cvm_headers_changed(cvm_files=[f[1] for f in cvm_files]):
#                 raise ValueError('Headers Changed! Update not possible.')

#             step = 'UPDATE CVM HISTORY DATA'
#             update_time = datetime.now()

#             cvm_files = [[f[0], f[1], update_time] for f in cvm_files]

#             if PARALLELIZE:
#                 with Pool(os.cpu_count()) as p:
#                     results = p.map(_process_cvm_history_file, cvm_files)
#             else:
#                 for cvm_file in cvm_files:
#                     result = _process_cvm_history_file(cvm_file)
#                     results.append(result)

#             step = 'UPDATE CVM CATALOG JOURNAL: CONSOLIDATING INFO'
#             result_cols = [
#                 'name', 'kind', 'sub_kind', 'cvm_file', 'update_time', 'records', 
#                 'partition_cols', 'timelapse', 'last_step', 'status', 'message'
#             ]
#             result_data = pd.DataFrame(
#                 [r for r in [r[0] for r in results]], columns=result_cols
#             )
#             result_table = 'cvm_update_history_results_stg'
#             result_data.to_sql(result_table, con=STAGE, index=False, if_exists='replace')

#             step = 'UPDATE CVM CATALOG JOURNAL: UPDATING DATA'
#             for name, kind, update_time in pd.read_sql(f'''
#                 with t as (
#                     select name, kind,
#                         sum(case when status='ERROR' then 1 else 0 end) errors, 
#                         sum(case when status='SUCCESS' then 1 else 0 end) successes,
#                         max(update_time) update_time
#                     from {result_table}
#                     group by name, kind
#                 )
#                 select name, kind, update_time
#                 from t
#                 where errors = 0
#                 and successes > 0
#             ''', con=STAGE).to_records(index=False):
#                 _ = self.CATALOG.execute(f'''
#                     update {self.CATALOG_JOURNAL}
#                     set last_updated = '{update_time}',
#                         process = FALSE
#                     where name = '{name}'
#                     and kind = '{kind}';
#                 ''')
#                 sleep(0.5)

#         return results
#     except Exception as E:
#         info = debug_info(E)
#         raise ValueError('Fail to get CVM UPDATE HISTORY DATA Data on step {}: {} ({}) (name={}, file={})'.format(
#             step, E, info, name, cvm_file))
#     finally:
#         STAGE = None


class CVM():
    @staticmethod
    def check_history_folder(history_folder=None):
        """
        Checks if the history folder exists and creates it if it doesn't.
            Args:
            history_folder (str, optional): The path to the history folder. Defaults to None.
            Returns:
            str: The path to the history folder.
            Return Value or Values and their Purpose:
            - history_folder (str): The path to the history folder. This is the same as the input history_folder parameter if provided,
                otherwise it is the default path to the history folder.
            Overview of General Functionality:
            The _check_history_folder function is responsible for ensuring the existence of the history folder. It allows the 
            flexibility to provide a custom path or use a default path. If the history folder doesn't exist, it creates it.
        """
        history_folder = history_folder or os.path.sep.join([FI.USER_APP_FOLDER, 'history'])
        if not os.path.exists(history_folder):
            os.makedirs(history_folder)
        
        return history_folder

    def __init__(self, catalog=None, history_folder=None):
        """
        Initializes an instance of the class.
        Args:
            catalog (Optional[sqlite3.Connection]): A database connection to a catalog.
                Defaults to None.
            history_folder (Optional[str]): The path to a history folder.
                Defaults to None.
        Returns:
            None
        Code Overview:
        The code initializes the attributes of the class by either creating new database connections
        or assigning the provided connections. It also sets up the CATALOG_JOURNAL attribute with a specific string value.
        The purpose of this initialization is to prepare the class for further operations and interactions
        with databases and folders.
        """
        if catalog is None or not FI.is_valid_db_connection(catalog):
            self.CATALOG = sqlite3.connect(os.path.sep.join([FI.USER_APP_FOLDER, 'catalog.db']))
        else:
            self.CATALOG = catalog

        self.HISTORY_FOLDER = CVM.check_history_folder(history_folder)
        
        self.CATALOG_JOURNAL = 'cvm_if_catalog_journal'


    def __del__(self):
        """
        Performs cleanup actions when the object is about to be destroyed.
        Args:
            None
        Returns:
            None
        Functionality Description:
            - Attempts to close the database connection stored in the CATALOG attribute.
            - If an exception occurs during the closing operation, it is ignored.
            - Sets the CATALOG attribute to None, indicating that the database connection is no longer valid.
        Code Overview:
        The code defines a destructor method that is automatically called when the object is about to be destroyed.
        It is responsible for closing the database connection stored in the CATALOG attribute and setting the attribute to None.
        The purpose of this cleanup is to ensure that resources are properly released and avoid potential memory leaks.
        """
        if FI.is_valid_db_connection(self.CATALOG):
            self.CATALOG.close()
        self.CATALOG = None
    

    # @debug
    def get_cvm_catalog(self):
        """
        Retrieves catalog data from a SQL database.
        Returns:
        - DataFrame: A pandas DataFrame containing the catalog data.
        Raises:
        - Exception: If there is an error retrieving the catalog data.
        Dependencies:
        - pandas
        Example Usage:
          catalog_data = get_cvm_catalog()
        """
        try:
            return pd.read_sql(f"""
                select *
                  from {self.CATALOG_JOURNAL}
            """, con=self.CATALOG)
        except Exception:
            return None

    
    # @debug
    def update_cvm_catalog(self):
        """
        Updates the history files in the CVM (Comissão de Valores Mobiliários) catalog.
        Returns:
            list: A list of results from the update process. Each result is a tuple containing the name, kind, status,
            and message of the processed file.
        Overview:
        This function updates the history files in the CVM catalog. It sets up components, sets up the catalog journal,
        and gets the list of remote files. It then updates the catalog journal with the metadata of the remote files, and
        processes the files. If the 'parallelize' argument is True and the number of CPUs is more than 1, it uses parallel
        processing. After processing the files, it consolidates the information and updates the CVM catalog journal.
        Note: If an exception occurs during the process, it raises a ValueError with the step at which the
        error occurred and the error message.
        """
        step = 'SETTING UP COMPONENTS'
        STAGE = sqlite3.connect(':memory:')
        try:
            step = "SETTING UP CATALOG JOURNAL AND RETURN METADATA"

            if_remote_files = pd.concat([
                _get_remote_files_list(
                    'IF_REGISTER', URL_IF_REGISTER, URL_IF_REGISTER_HIST
                ),
                _get_remote_files_list(
                    'IF_POSITION', URL_IF_DAILY, URL_IF_DAILY_HIST
                )
            ])

            if_remote_files['url'] = if_remote_files.apply(
                lambda x:'%s/%s' % (x['url'], x['href']), axis=1
            )

            if_remote_files.to_sql(
                'if_remote_files', index=False, con=STAGE, if_exists='replace'
            )

            catalog_journal_df = self.get_cvm_catalog()

            if type(catalog_journal_df) != pd.DataFrame:
                catalog_journal_df = pd.read_sql('''
                        select 
                            sequence, 
                            href, 
                            name, 
                            last_modified, 
                            size, 
                            history, 
                            url,
                            kind,
                            NULL last_download,
                            NULL last_updated,
                            1 process,
                            1 active
                        from if_remote_files
                        where 1 = 0
                    ''', con=STAGE)

            catalog_journal_df.to_sql(
                'catalog_journal', index=False, con=STAGE, if_exists='replace')

            catalog_journal_df = pd.read_sql("""
                select r.sequence, 
                    r.href, 
                    r.name, 
                    r.last_modified, 
                    r.size, 
                    r.history, 
                    r.url,
                    r.kind,
                    NULL last_download,
                    NULL last_updated,
                    1 process,
                    1 active
                from catalog_journal c
                full outer join if_remote_files r 
                using (href, name, kind)
                where c.sequence is null
                union
                select c.sequence, 
                    c.href, 
                    c.name, 
                    c.last_modified, 
                    c.size, 
                    c.history, 
                    c.url,
                    c.kind,
                    c.last_download,
                    c.last_updated,
                    0 process,
                    0 active
                from catalog_journal c
                full outer join if_remote_files r 
                using (href, name, kind)
                where r.sequence is null
                union
                select r.sequence, 
                    r.href, 
                    r.name, 
                    r.last_modified, 
                    r.size, 
                    r.history, 
                    r.url,
                    r.kind,
                    c.last_download,
                    c.last_updated,
                    case 
                        when r.last_modified is null then 0
                        when c.last_download is null then 1
                        when r.last_modified > c.last_download then 1 
                        else 0 
                    end process,
                    c.active
                from catalog_journal c
                full outer join if_remote_files r 
                using (href, name, kind)
                where r.sequence is not null
            """, con=STAGE)

            catalog_journal_df.to_sql(self.CATALOG_JOURNAL, con=self.CATALOG, if_exists='replace', index=False)

            results = []
            metadata_to_process = []
            for m in catalog_journal_df.to_dict(orient='records'):
                if m['process']:
                    m['history_folder'] = self.HISTORY_FOLDER
                    metadata_to_process.append(m)

            for if_metadata in metadata_to_process:
                result = _update_cvm_history_file(if_metadata)
                results.append(result)

            step = 'UPDATE CVM CATALOG JOURNAL: CONSOLIDATING INFO'
            result_cols = [
                'name', 'kind', 'status', 'message'
            ]
            result_data = pd.DataFrame(
                [(r[0][1]['name'], r[0][1]['kind'], r[0][0], r[0][-1]) for r in results if r[0][0] != 'SKIP'], columns=result_cols
            )
            result_table = 'cvm_update_history_file_results_stg'
            result_data.to_sql(result_table, con=STAGE, index=False, if_exists='replace')

            download_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            step = 'UPDATE CVM CATALOG JOURNAL: UPDATING DOWNLOAD DATE'
            update_results = pd.read_sql(f'''
                with t as (
                    select name, kind,
                           sum(case when status='ERROR' then 1 else 0 end) errors, 
                           sum(case when status='SUCCESS' then 1 else 0 end) successes, 
                           sum(case when status='SKIP' then 1 else 0 end) skips
                      from {result_table}
                     group by name, kind
                )
                select name, kind, errors, successes, skips
                  from t
            ''', con=STAGE)

            ops = []
            for name, kind, errors, successes, skips in update_results.to_records(index=False):
                if errors == 0 and (successes > 0 or skips > 0):
                    _sql = f'''
                        update {self.CATALOG_JOURNAL}
                        set last_download = '{download_time}',
                            process = FALSE
                        where name = '{name}'
                        and kind = '{kind}'
                        and process
                        and active;
                    '''
                    _ = self.CATALOG.execute(_sql)
                    ops.append((_sql, _))

            return update_results.to_dict(orient='records'), metadata_to_process, ops
        except Exception as E:
            info = debug_info(E)
            raise ValueError('Fail to UPDATE history files on step {}: {} ({})'.format(step, E, info))
        finally:
            STAGE = None


    def get_cvm_files(self, kind='IF_POSITION', history=False):
        """
        Get CVM files to process from the CVM catalog.
        Args:
            kind (str, optional): The kind of CVM files to retrieve. Defaults to 'IF_POSITION'.
            history (bool, optional): Flag indicating whether to include historical files. Defaults to False.
        Returns:
            tuple: A tuple of results containing information about the CVM files.
                Each element in the tuple is a tuple containing:
                    - kind (str): The kind of CVM file.
                    - name (str): The name of the CVM file.
                    - history (bool): Flag indicating if the file is historical.
                    - files (tuple): A tuple of file paths matching the specified kind and name.
        Overview:
            This function retrieves CVM files to process from the CVM catalog based on the provided kind and history
            parameters.
            It queries the database table specified by self.CATALOG_JOURNAL to fetch the relevant files.
            The SQL query filters the files based on the kind, history, and last_updated criteria.
            The function then uses the FU.find utility function to find the matching files in the HISTORY_FOLDER.
            The retrieved files are appended to the result list as tuples, each containing the file's kind, name,
            history flag, and file paths.
            Finally, the function returns the result as a tuple.
        """
        step = 'GET CVM FILES TO PROCESS FROM CVM CATALOG'
        try:
            result = []
            for kind, name, history in pd.read_sql(f'''
                select kind, name, history
                from {self.CATALOG_JOURNAL}
                where active
                and kind = '{kind}'
                and history = {history}
                and last_updated is NULL 
                    or last_download > last_updated
                order by name
            ''', con=self.CATALOG).to_records(index=False):
                files = FU.find(self.HISTORY_FOLDER, f'{kind.lower()}.{name.lower()}.*')
                result.append((kind, name, bool(history), tuple(files)))
            return tuple(result)
        except Exception as E:
            info = debug_info(E)
            raise ValueError('Failed to get CVM files from catalog on step {}: {} ({})'.format(step, E, info))
    

    def get_cvm_file_data(self, cvm_file):
        """
        Get data from a CVM file.
        Args:
            cvm_file (str): The path to the CVM file.
        Returns:
            list: A list of dictionaries containing the data from the CVM file.
        Overview:
            This function reads the data from the specified CVM file using the _read_cvm_history_file
            function and returns the data as a list of dictionaries. The _read_cvm_history_file function
            is a private function that is not exposed to the user. It reads the data from the CVM file
            and returns it as a list of dictionaries. The get_cvm_file_data function simply calls the
            _read_cvm_history_file function and returns its result. If the specified CVM file does not
            exist, an exception is raised.
        """
        return _read_cvm_history_file(cvm_file)
    

    def update_cvm_files(self, cvm_files):
        """
        Update the CVM catalog journal by updating the last update date for each CVM file.
        Args:
            cvm_files (list): A list of tuples containing information about each CVM file.
                Each tuple should contain (kind, name, last_update) where:
                    - kind (str): The kind of CVM file.
                    - name (str): The name of the CVM file.
                    - last_update (str): The last update date for the CVM file.
        Returns:
            bool: True if the CVM catalog is successfully updated, False otherwise.
        Raises:
            ValueError: If there is a failure in updating the CVM catalog. The exception message
                will provide detailed information about the failure.
        Overview:
            This function updates the CVM catalog journal by setting the last_updated field
            for each CVM file that matches the given kind and name. It iterates through the
            cvm_files list and constructs an SQL update statement for each file. The update
            statement sets the last_updated field to the provided last_update value. The update
            is performed using the CATALOG object's execute method. If any exception occurs
            during the update process, a ValueError is raised with a detailed error message.
        """
        step = 'UPDATE CVM CATALOG JOURNAL: UPDATING UPDATE DATE FOR CVM FILE'
        try:
            for kind, name, last_update in cvm_files:
                _sql = f"""
                    update {self.CATALOG_JOURNAL}
                    set last_updated = '{last_update}'
                    where active
                    and process
                    and kind = '{kind}'
                    and name = '{name}'
                """
                _ = self.CATALOG.execute(_sql)

            return True
        except Exception as E:
            info = debug_info(E)
            raise ValueError('Fail to UPDATE CVM CATALOG on step {}: {} ({})'.format(step, E, info))
