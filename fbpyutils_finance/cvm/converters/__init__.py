"""
Converters for the CVM data.
"""
import pandas as pd
from datetime import datetime


def is_nan_or_empty(x):
    """
    Checks if a value is NaN (Not a Number) or empty.
     Args:
        x (any): The value to check.
     Returns:
        bool: True if the value is NaN or empty, False otherwise.
    """
    return not x or pd.isna(x) or len(str(x)) == 0


def as_boolean(x):
    """
    Converts a value to a boolean.
     Args:
        x (any): The value to convert.
     Returns:
        bool: True if the value is 'S', 'Y', or '1' (case-insensitive), False otherwise.
    """
    if is_nan_or_empty(x):
        return False
    else:
        return str(x).upper() in ('S', 'Y', '1')


def as_date(x):
    """
    Converts a string representation of a date to a `datetime.date` object.
     Args:
        x (str): The string representation of the date in the format 'YYYY-MM-DD'.
     Returns:
        datetime.date or None: A `datetime.date` object representing the date, or None if the input is NaN or empty.
    """
    if is_nan_or_empty(x):
        return None
    else:
        return datetime.strptime(x, '%Y-%m-%d').date()


def as_float(x):
    """
    Converts a value to a floating-point number.
     Args:
        x (any): The value to convert.
     Returns:
        float or None: A floating-point number representation of the input, or None if the input is NaN or empty,
        or cannot be converted to a float.
    """
    if is_nan_or_empty(x):
        return None
    else:
        try:
            return float(str(x))
        except ValueError:
            return None


def as_integer(x):
    """
    Converts a value to an integer.
     Args:
        x (any): The value to convert.
     Returns:
        int or None: An integer representation of the input, or None if the input is NaN or empty,
        or cannot be converted to an integer.
    """
    if is_nan_or_empty(x):
        return 0
    else:
        try:
            return int(float(x))
        except ValueError:
            return None


def as_string(x):
    """
    Converts a value to a string.
     Args:
        x (any): The value to convert.
     Returns:
        str or None: A string representation of the input, or None if the input is NaN or empty.
    """
    if is_nan_or_empty(x):
        return None
    else:
        return str(x)


def as_string_id(x):
    """
    Converts a value to a string and removes specific characters (/,-,.).
     Args:
        x (any): The value to convert.
     Returns:
        str or None: A string representation of the input with specific characters removed,
        or None if the input is NaN or empty.
    """
    if is_nan_or_empty(x):
        return None
    else:
        return str(x).replace('/', '').replace('-', '').replace('.', '')