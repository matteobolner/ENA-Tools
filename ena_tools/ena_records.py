import requests
import pandas as pd
import io

def get_record_types(format:str='tsv') -> pd.DataFrame:
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

def get_fields_available_for_record_type(record_type: str, format:str='tsv' ) -> pd.DataFrame:
    valid_record_types = ['analysis', 'analysis_study','assembly','coding','noncoding','read_experiment','read_run','read_study','sample','sequence','study','taxon']
    invalid_record_types= ['tls_set','tsa_set','wgs_set']
    if record_type not in valid_record_types:
        if record_type in invalid_record_types:
            raise ValueError(f"{record_type} is not accepted by the ENA api even if it is listed as result type \n Please choose one of: " + ', '.join(valid_record_types))
        else:
            raise ValueError("Invalid result type. Please choose one of: " + ', '.join(valid_record_types))

    headers = {
        'accept': '*/*',
    }

    params = {
        'dataPortal': 'ena', # doesnt matter which portal you choose, the results are the same
        'result': record_type, #invalid result types are : 'tls_set','tsa_set','wgs_set'
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


a=get_fields_available_for_record_type("sample")




#def get_ena_records(record_type, )


'''

def build_ena_reads_db(taxon, include_subordinate_taxa, n_samples, format='tsv', fields='all'):
    """
    Query ENA for all reads belonging to the input taxa
    """
    if include_subordinate_taxa == True:
        query_tax = "tax_tree"
        print(f"Searching ENA for taxon {taxon} and subordinate taxa...")
    else:
        query_tax = "tax_eq"
        print(f"Searching ENA for taxon {taxon}...")

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    result='read_run'
    query=f"{query_tax}({taxon})"
    if fields == 'all':
        fields="run_accession%2Cexperiment_title%2Ctax_id%2Cage%2Caligned%2Caltitude%2Cassembly_quality%2Cassembly_software%2Cbam_aspera%2Cbam_bytes%2Cbam_ftp%2Cbam_galaxy%2Cbam_md5%2Cbase_count%2Cbinning_software%2Cbio_material%2Cbisulfite_protocol%2Cbroad_scale_environmental_context%2Cbroker_name%2Ccage_protocol%2Ccell_line%2Ccell_type%2Ccenter_name%2Cchecklist%2Cchip_ab_provider%2Cchip_protocol%2Cchip_target%2Ccollected_by%2Ccollection_date%2Ccollection_date_end%2Ccollection_date_start%2Ccompleteness_score%2Ccontamination_score%2Ccontrol_experiment%2Ccountry%2Ccultivar%2Cculture_collection%2Cdatahub%2Cdepth%2Cdescription%2Cdev_stage%2Cdisease%2Cdnase_protocol%2Cecotype%2Celevation%2Cenvironment_biome%2Cenvironment_feature%2Cenvironment_material%2Cenvironmental_medium%2Cenvironmental_sample%2Cexperiment_accession%2Cexperiment_alias%2Cexperiment_target%2Cexperimental_factor%2Cexperimental_protocol%2Cextraction_protocol%2Cfaang_library_selection%2Cfastq_aspera%2Cfastq_bytes%2Cfastq_ftp%2Cfastq_galaxy%2Cfastq_md5%2Cfile_location%2Cfirst_created%2Cfirst_public%2Cgermline%2Chi_c_protocol%2Chost%2Chost_body_site%2Chost_genotype%2Chost_gravidity%2Chost_growth_conditions%2Chost_phenotype%2Chost_scientific_name%2Chost_sex%2Chost_status%2Chost_tax_id%2Cidentified_by%2Cinstrument_model%2Cinstrument_platform%2Cinvestigation_type%2Cisolate%2Cisolation_source%2Clast_updated%2Clat%2Clibrary_construction_protocol%2Clibrary_gen_protocol%2Clibrary_layout%2Clibrary_max_fragment_size%2Clibrary_min_fragment_size%2Clibrary_name%2Clibrary_pcr_isolation_protocol%2Clibrary_prep_date%2Clibrary_prep_date_format%2Clibrary_prep_latitude%2Clibrary_prep_location%2Clibrary_prep_longitude%2Clibrary_selection%2Clibrary_source%2Clibrary_strategy%2Clocal_environmental_context%2Clocation%2Clocation_end%2Clocation_start%2Clon%2Cmarine_region%2Cmating_type%2Cncbi_reporting_standard%2Cnominal_length%2Cnominal_sdev%2Cpcr_isolation_protocol%2Cph%2Cproject_name%2Cprotocol_label%2Cread_count%2Cread_strand%2Crestriction_enzyme%2Crestriction_enzyme_target_sequence%2Crestriction_site%2Crna_integrity_num%2Crna_prep_3_protocol%2Crna_prep_5_protocol%2Crna_purity_230_ratio%2Crna_purity_280_ratio%2Crt_prep_protocol%2Crun_alias%2Crun_date%2Csalinity%2Csample_accession%2Csample_alias%2Csample_capture_status%2Csample_collection%2Csample_description%2Csample_material%2Csample_prep_interval%2Csample_prep_interval_units%2Csample_storage%2Csample_storage_processing%2Csample_title%2Csampling_campaign%2Csampling_platform%2Csampling_site%2Cscientific_name%2Csecondary_project%2Csecondary_sample_accession%2Csecondary_study_accession%2Csequencing_date%2Csequencing_date_format%2Csequencing_location%2Csequencing_longitude%2Csequencing_method%2Csequencing_primer_catalog%2Csequencing_primer_lot%2Csequencing_primer_provider%2Cserotype%2Cserovar%2Csex%2Cspecimen_voucher%2Csra_aspera%2Csra_bytes%2Csra_ftp%2Csra_galaxy%2Csra_md5%2Cstatus%2Cstrain%2Cstudy_accession%2Cstudy_alias%2Cstudy_title%2Csub_species%2Csub_strain%2Csubmission_accession%2Csubmission_tool%2Csubmitted_aspera%2Csubmitted_bytes%2Csubmitted_format%2Csubmitted_ftp%2Csubmitted_galaxy%2Csubmitted_host_sex%2Csubmitted_md5%2Csubmitted_read_type%2Ctag%2Ctarget_gene%2Ctaxonomic_classification%2Ctaxonomic_identity_marker%2Ctemperature%2Ctissue_lib%2Ctissue_type%2Ctransposase_protocol%2Cvariety"
    data = f'result={result}&query={query}&fields={fields}&format={format}'

    r = requests.post(
        "https://www.ebi.ac.uk/ena/portal/api/search", headers=headers, data=data
    )
    print("Data obtained, now formatting...")
    df = pd.read_table(io.StringIO(r.text))
    df=df.dropna(how="all", axis=1)

    emptycols = []
    for col in df.columns:
        if len(df[col].unique()) == 1 and df[col].unique()[0] == "":
            emptycols.append(col)
    df = df.drop(emptycols, axis=1)
    print(f"All ENA reads for taxon {taxon} obtained (n={len(df)}) over {len(df['sample_accession'].unique())}")
    if n_samples=='all':
        return(df)
    else:
        return df.sample(n_samples)
'''
