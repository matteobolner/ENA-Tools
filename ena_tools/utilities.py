import pandas as pd
import unicodedata

def split_df_in_rows(self, df):
    rows_list = [row for _, row in df.iterrows()]
    return rows_list

def remove_diacritics(self, input_string):
    input_string=str(input_string)
    return ''.join(
        char for char in unicodedata.normalize('NFD', input_string)
        if unicodedata.category(char) != 'Mn'  # Filter out combining characters
    )

def strip_string_down_to_letters(self, input_string):
    stripped_string=self.remove_diacritics(input_string)
    #stripped_string=re.sub(r'\W+', '', stripped_string)
    stripped_string = re.sub(r'[^a-zA-Z0-9\s]+', '', stripped_string)
    stripped_string=stripped_string.lower()
    return(stripped_string)
