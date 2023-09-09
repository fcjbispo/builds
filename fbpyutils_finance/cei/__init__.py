import os
import re
import sqlite3
import pandas as pd

from datetime import datetime
from multiprocessing import Pool

import warnings

from fbpyutils import file as FU, xlsx as XL

from fbpyutils_finance.cei.schemas import \
    process_schema_movimentacao, process_schema_eventos_provisionados, process_schema_negociacao, \
    process_schema_posicao_acoes, process_schema_posicao_emprestimo_ativos, process_schema_posicao_etf, \
    process_schema_posicao_fundos_investimento, process_schema_posicao_tesouro_direto, \
    process_schema_posicao_renda_fixa

warnings.simplefilter("ignore")

_OPERATIONS = (
    ('movimentacao', 'movimentacao-*.xlsx', process_schema_movimentacao, True),
    ('eventos_provisionados', 'eventos-*.xlsx', process_schema_eventos_provisionados, True),
    ('negociacao', 'negociacao-*.xlsx', process_schema_negociacao, True),
    ('posicao_acoes', 'posicao-*.xlsx', process_schema_posicao_acoes, True),
    ('posicao_emprestimo_ativos', 'posicao-*.xlsx', process_schema_posicao_emprestimo_ativos, True),
    ('posicao_etf', 'posicao-*.xlsx', process_schema_posicao_etf, True),
    ('posicao_fundos_investimento', 'posicao-*.xlsx', process_schema_posicao_fundos_investimento, True),
    ('posicao_tesouro_direto', 'posicao-*.xlsx', process_schema_posicao_tesouro_direto, True),
    ('posicao_renda_fixa', 'posicao-*.xlsx', process_schema_posicao_renda_fixa, True),
)


def _process_operation(operation):
    """
    Process the specified operation.
     Args:
        operation (tuple): A tuple containing the details of the operation to be processed.
            - op_name (str): The name of the operation.
            - input_folder (str): The path to the folder containing the input files.
            - input_mask (str): The mask or pattern for finding input files.
            - processor (function): The function to be applied to the input files.
     Returns:
        tuple: A tuple containing the result of the operation.
            - op_name (str): The name of the operation.
            - num_files (int): The number of input files processed.
            - data (any): The processed data.
     Overview:
        This function processes the specified operation by performing the following steps:
        1. Extract the operation details from the input tuple.
        2. Use the `FU.find` function to find input files in the specified input folder using the input mask.
        3. Apply the specified `processor` function to the input files to obtain the processed data.
        4. Return a tuple containing the operation name, the number of input files processed, and the processed data.
     Example usage:
        operation = ('op_name', '/path/to/input_folder', '*.txt', process_function)
        result = _process_operation(operation)
    """
    op_name, input_folder, input_mask, processor = operation
    input_files = FU.find(input_folder, input_mask)
    data = processor(input_files)

    rows = 0 if data is None else len(data)

    return (op_name, rows, data)


def get_cei_data(input_folder, parallelize=True):
    """
    Retrieves CEI data from the specified input folder and processes it based on the specified operations.
     Args:
        input_folder (str): The path to the folder containing the CEI data.
        parallelize (bool, optional): Flag indicating whether to parallelize the data processing. Defaults to True.
     Returns:
        list: A list containing the processed data.
            Each item in the list represents the result of processing an operation and consists of:
            - operation (str): The name of the operation performed.
            - data (any): The processed data.
     Overview:
        This function retrieves CEI data from the specified input folder and processes it based on the operations defined in `_OPERATIONS`.
        The `parallelize` parameter controls whether the data processing is parallelized.
        The function first checks if parallelization is enabled by comparing the value of `parallelize` with the number of available CPUs.
        It then initializes an empty list to store the results of the data processing.
        If parallelization is enabled, the function uses a multiprocessing pool to concurrently process the data using the `_process_operation` function.
        Otherwise, it sequentially processes the data by iterating over the operations and calling `_process_operation` for each operation.
        The results of the data processing are appended to the data list.
        Finally, the function returns the list containing the processed data.
     Example usage:
        data = _get_cei_data_pre(input_folder='/path/to/cei_data', parallelize=True)
    """
    PARALLELIZE = parallelize and os.cpu_count()>1
    operations = []

    for op, mask, processor, enabled in _OPERATIONS:
        if enabled: 
            operations.append((op, input_folder, mask, processor,))
    
    operations = tuple(operations)
    
    if PARALLELIZE:
        with Pool(os.cpu_count()) as p:
            data = p.map(_process_operation, operations)
    else:
        data = []
        for operation in operations:
            data.append(_process_operation(operation))
    
    return data
