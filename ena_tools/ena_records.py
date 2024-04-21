import requests
import pandas as pd
import io

def get_result_types(format:str='tsv') -> pd.DataFrame:
    """
    Get available result types (data sets) to search against.
    """
    valid_formats = ['tsv', 'json']
    if format not in valid_formats:
        raise ValueError("Invalid format. Please choose one of: " + ', '.join(valid_formats))
    headers = {
        'accept': '*/*',
    }

    params = {
        'dataPortal': 'ena', # doesnt matter which portal you choose, the results are the same
        'format': format,
    }
    response = requests.get('https://www.ebi.ac.uk/ena/portal/api/results', params=params, headers=headers)
    if response.ok:
        if format=='tsv':
            fields=pd.read_table(io.StringIO(response.text))
        else:
            fields=pd.DataFrame(response.json())
        return fields
    else:
        response.raise_for_status()

def get_fields_available_for_result_type(result_type: str, format:str='tsv' ) -> pd.DataFrame:
    """
    Get fields that can be returned for a result type.
    """

    valid_result_types = ['analysis', 'analysis_study','assembly','coding','noncoding','read_experiment','read_run','read_study','sample','sequence','study','taxon']
    invalid_result_types= ['tls_set','tsa_set','wgs_set']
    if result_type not in valid_result_types:
        raise ValueError("Invalid result type. Please choose one of: " + ', '.join(valid_result_types))

    headers = {
        'accept': '*/*',
    }

    params = {
        'dataPortal': 'ena', # doesnt matter which portal you choose, the results are the same
        'result': result_type, #invalid result types are : 'tls_set','tsa_set','wgs_set'
        'format': format,
    }
    response = requests.get('https://www.ebi.ac.uk/ena/portal/api/returnFields', params=params, headers=headers)
    if response.ok:
        if format=='tsv':
            fields=pd.read_table(io.StringIO(response.text))
        else:
            fields=pd.DataFrame(response.json())
        return fields
    else:
        return response.raise_for_status()

def get_accession_types_available_for_result_type(result_type: str) -> pd.DataFrame:
    """
    Get accession types that can be used in the search query.
    """
    valid_result_types = ['analysis', 'analysis_study','assembly','coding','noncoding','read_experiment','read_run','read_study','sample','sequence','study','taxon','tls_set','tsa_set','wgs_set']
    if result_type not in valid_result_types:
        raise ValueError("Invalid result type. Please choose one of: " + ', '.join(valid_result_types))

    headers = {
        'accept': '*/*',
    }

    params = {
        'result': result_type,
    }

    response = requests.get('https://www.ebi.ac.uk/ena/portal/api/accessionTypes', params=params, headers=headers)
    if response.ok:
        accession_types=pd.DataFrame(response.json())['accessionType'].tolist()
        return accession_types
    else:
        return response.raise_for_status()


def ena_query(result_type:str, query:str, format:str='tsv',fields:str|list='all', limit=0,download=False,includeMetagenomes=False,dataPortal=None,includeAccessionType=[],includeAccessions=[],excludeAccessionType=[],excludeAccessions=[], dccDataOnly=False, rule=None):
    if fields=='all':
        fields=get_fields_available_for_result_type(result_type)['columnId'].tolist()

    params={
        'result':result_type,
        'query':query,
        'format':format,
        'fields':'%2C'.join(fields),
        'limit':limit,
        'download': 'true' if download else 'false',
        'includeMetagenomes':'true' if includeMetagenomes else 'false',
        'dataPortal':dataPortal,
        'includeAccessionType':'%2C'.join(includeAccessionType),
        'includeAccessions':'%2C'.join(includeAccessions),
        'excludeAccessions':'%2C'.join(excludeAccessions),
        'excludeAccessionType':'%2C'.join(excludeAccessionType),
        'dccDataOnly':'true' if dccDataOnly else 'false',
        'rule':rule,
    }

    data=''
    for param_name,param_value in params.items():
        if param_value:
            data+=f'{param_name}={param_value}&'
        else:
            data+=f'{param_name}=&'

    data=data.rstrip("&")

    headers = {
        'accept': '*/*',
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    response = requests.post('https://www.ebi.ac.uk/ena/portal/api/search', headers=headers, data=data)
    if response.ok:
        if format=='tsv':
            records=pd.read_table(io.StringIO(response.text))
        else:
            records=pd.DataFrame(response.json())
        return records
    else:
        return response.raise_for_status()
