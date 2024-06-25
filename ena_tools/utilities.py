import pandas as pd
import unicodedata
import re

def split_df_in_rows(df):
    rows_list = [row for _, row in df.iterrows()]
    return rows_list

def remove_diacritics(input_string):
    input_string=str(input_string)
    return ''.join(
        char for char in unicodedata.normalize('NFD', input_string)
        if unicodedata.category(char) != 'Mn'  # Filter out combining characters
    )

def strip_string_down_to_letters(input_string):
    stripped_string=remove_diacritics(input_string)
    #stripped_string=re.sub(r'\W+', '', stripped_string)
    stripped_string = re.sub(r'[^a-zA-Z0-9\s]+', '', stripped_string)
    stripped_string=stripped_string.lower()
    return(stripped_string)
