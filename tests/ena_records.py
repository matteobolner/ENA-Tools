import requests
import pandas as pd
import io
import pytest
from ena_tools.ena_records import get_result_types, get_fields_available_for_result_type, get_accession_types_available_for_result_type, ena_query



class TestGetEnaResulttypes:
    @pytest.mark.parametrize("format", ['tsv', 'json'])

    def test_valid_url(self, format):
        result = get_result_types(format)

        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert len(result) >= 1
        assert list(result.columns)==['resultId', 'description', 'primaryAccessionType', 'recordCount', 'lastUpdated']

class TestGetFieldsAvailableForResulttype:
    @pytest.mark.parametrize("format", ['tsv', 'json'])
    @pytest.mark.parametrize("result_type", ['analysis', 'analysis_study','assembly','coding','noncoding','read_experiment','read_run','read_study','sample','sequence','study','taxon'])
    def test_valid_url(self, result_type, format):
        result = get_fields_available_for_result_type(result_type, format)
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert len(result) >= 1
        assert list(result.columns)==['columnId', 'description', 'type']

class TestGetAccessionTypesAvailableForResulttype:
    @pytest.mark.parametrize("result_type", ['analysis', 'analysis_study','assembly','coding','noncoding','read_experiment','read_run','read_study','sample','sequence','study','taxon','tls_set','tsa_set','wgs_set'])
    def test_valid_url(self, result_type):
        result = get_accession_types_available_for_result_type(result_type)
        assert isinstance(result, list)
        assert len(result) >= 1


class TestEnaQuery:
    @pytest.mark.parametrize("query", ['tax_tree(9903)', 'tax_eq(9913)'])
    @pytest.mark.parametrize("format", ['tsv', 'json'])
    @pytest.mark.parametrize("limit", [10, 1000])
    @pytest.mark.parametrize("result_type", ['analysis', 'analysis_study','assembly','coding','noncoding','read_experiment','read_run','read_study','sample','sequence','study','taxon','tls_set','tsa_set','wgs_set'])

    def test_valid_url(self, query, format, limit, result_type):
        if result_type in ['tls_set','tsa_set','wgs_set']:
            with pytest.raises(ValueError):
                result = ena_query(query=query, result_type=result_type, format=format, limit=limit)
        else:
            result = ena_query(query=query, result_type=result_type, format=format, limit=limit)
            assert isinstance(result, pd.DataFrame)
            if len(result)==0:
                print(query, format, limit, result_type)
            assert len(result) <= limit #### if there are less available values than the limit test for equal size will fail even if code successful
