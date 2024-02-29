import requests
import pandas as pd
import io
import pytest
from ena_tools.ena_records import get_record_types,get_fields_available_for_record_type


class TestGetEnaResultTypes:
    @pytest.mark.parametrize("format", ['tsv', 'json'])

    def test_valid_url(self, format):
        result = get_record_types(format)

        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert len(result) >= 1
        assert list(result.columns)==['resultId', 'description', 'primaryAccessionType', 'recordCount', 'lastUpdated']

class TestGetFieldsAvailableForResultType:
    @pytest.mark.parametrize("format", ['tsv', 'json'])
    @pytest.mark.parametrize("record_type", ['analysis', 'analysis_study','assembly','coding','noncoding','read_experiment','read_run','read_study','sample','sequence','study','taxon'])
    def test_valid_url(self, record_type, format):
        result = get_fields_available_for_record_type(record_type, format)
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert len(result) >= 1
        assert list(result.columns)==['columnId', 'description', 'type']
