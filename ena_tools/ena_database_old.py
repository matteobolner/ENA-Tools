import requests
import pandas as pd
from tqdm import tqdm
import io
import re
import numpy as np
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import unicodedata
tqdm.pandas()


class Taxon:
    def __init__(
        self,
        taxon,
        include_subordinate_taxa=True,
        ena_reads_db=None,
        biosamples_db=None,
        xrefs_db=None,
    ):








class EnaBiosamplesXrefDB:
    """
    Setup ena, biosamples and xref DB for taxon
    """
    def __init__(
        self,
        taxon,
        species,
        include_subordinate_taxa=True,
        ena_reads_db_path=None,
        biosamples_db_path=None,
        xrefs_db_path=None,
        n_samples='all', #for testing
    ):
        self.taxon = taxon
        self.species= species
        if ena_reads_db_path:
            print("Reading ENA db from file...")
            self.ena_reads_db=pd.read_table(ena_reads_db_path)
        else:
            self.ena_reads_db = self.build_ena_reads_db(taxon, include_subordinate_taxa, n_samples)

        self.samples=self.ena_reads_db['sample_accession'].dropna().unique().tolist()
        self.studies=self.ena_reads_db['study_accession'].dropna().unique().tolist()
        if biosamples_db_path:
            print("Reading BioSamples db from file...")
            self.biosamples_db=pd.read_table(biosamples_db_path)
        else:
            self.biosamples_db = self.build_biosamples_db(self.samples)
        if xrefs_db_path:
            print("Reading Xrefs db from file...")
            self.xrefs_db=pd.read_table(xrefs_db_path)
            print("Finished database setup")
        else:
            self.xrefs_db = self.build_study_xrefs_db(self.studies)

    def split_df_in_rows(self, df):
        rows_list = [row for _, row in df.iterrows()]
        return rows_list

    def build_study_xrefs_db(self, input, thread_number=4):
        if isinstance(input, list):
            study_ids = set(input)
        elif isinstance(input, pd.DataFrame):
            study_ids=set(input['study_accession'])
        else:
            raise Exception("Wrong format for Xrefs IDs: needs list or pandas Dataframe with column study_accession")

        pbar = tqdm(total=len(study_ids))
        study_ids=set(study_ids)
        def get_xref(study_id):
            pbar.update(1)
            headers = {"Content-Type": "application/json"}
            r = requests.get(
                "https://www.ebi.ac.uk/ena/xref/rest/json/search?accession="
                + study_id,
                headers=headers,
            )
            if r.ok:
                decoded = r.json()
                return(pd.DataFrame(decoded))
            else:
                return(np.nan)
        pool = ThreadPool(thread_number)
        xref_list = pool.map(get_xref, study_ids)
        pool.close()
        pool.join()
        xrefs_df=pd.concat(xref_list)
        return xrefs_df


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

    def get_fao_breeds_dataset(self):
        fao_dataset=pd.read_csv(f"https://raw.githubusercontent.com/matteobolner/livestock_breeds_database/main/species/{self.species}/all_breeds.csv")
        fao_dataset['breed_no_diacritics']=fao_dataset['Breed/Most common name'].dropna().apply(self.remove_diacritics)
        fao_dataset['breed_letters_only']=fao_dataset['Breed/Most common name'].apply(self.strip_string_down_to_letters)
        fao_dataset=fao_dataset[fao_dataset['Breed/Most common name'].apply(len)>=3]
        return(fao_dataset)

    def get_fao_breeds_stripped_list(self):
        fao_breeds_list=self.get_fao_breeds_dataset()['breed_letters_only'].unique().tolist()
        return(fao_breeds_list)

    def count_breed_occurrences_in_string(self, sample, breed_list, input_string):
        counts = {substring: len(re.findall(substring, input_string)) for substring in breed_list}
        counts = {k:v for k,v in counts.items() if v!=0}
        return(counts)

    def annotate_biosamples_with_breeds(self, breed_list='FAO', db='default', thread_number=4):
        if breed_list=='FAO':
            breed_list=self.get_fao_breeds_stripped_list()
        if isinstance(db, pd.DataFrame):
            biosamples=db
        elif (isinstance(db, str))&(db=='default'):
            biosamples=self.biosamples_db
        else:
            raise Exception("Unclear format for BioSamples DB")
        #biosamples=test
        datetimes_and_urls=[]
        counter=0
        for col in biosamples.columns:
            counter+=1
            if col.startswith("_links"):
                datetimes_and_urls.append(col)
            if biosamples[col].dtype == 'object':
                try:
                    pd.to_datetime(biosamples[col], format='mixed')
                    datetimes_and_urls.append(col)
                except:
                    pass
        biosamples=biosamples.drop(columns=datetimes_and_urls)
        biosamples=biosamples.drop(columns=['INSDC center name'])
        biosamples=biosamples.dropna(how='all', axis=1)
        biosamples=biosamples.dropna(how='all', axis=0)
        biosamples_stripped=biosamples.set_index('sample_accession').apply(lambda row: row.dropna().astype(str).str.cat(sep=' '), axis=1).apply(self.strip_string_down_to_letters)

        def annotate_sample_breed(sample_stripped_info):
            pbar.update(1)
            id=sample_stripped_info[0]
            info=sample_stripped_info[1]
            counts={id:self.count_breed_occurrences_in_string(sample=id, breed_list=breed_list, input_string=info)}
            return(counts)
        pbar = tqdm(total=len(biosamples))
        pool = ThreadPool(thread_number)
        annotated_samples = pool.map(annotate_sample_breed, list(biosamples_stripped.items()))
        pool.close()
        pool.join()
        annotated_samples_joined={}
        for i in annotated_samples:
            annotated_samples_joined.update(i)
        biosamples['breed_found_counts']=biosamples['sample_accession'].apply(lambda x:annotated_samples_joined[x])
        sample_breeds=biosamples[['sample_accession','organism','breed','breed_found_counts']]

        def remove_duplicates(input_dict):
            # Sort the dictionary by values in descending order
            sorted_dict_by_value = {k: v for k, v in sorted(input_dict.items(), key=lambda item: item[1], reverse=True)}
            sorted_dict_by_key = {}
            for k in sorted(sorted_dict_by_value, key=len, reverse=True):
                sorted_dict_by_key[k] = sorted_dict_by_value[k]

            # Create a list of keys from the sorted dictionary
            dict_keys = list(sorted_dict_by_key.keys())

            # Loop through the keys to identify and remove duplicates or overlapping keys
            for index, key in enumerate(dict_keys):
                for index2, key2 in enumerate(dict_keys[index+1:]):
                    if " ".join(sorted(key2.split())) in " ".join(sorted(key.split())):
                        try:
                            del sorted_dict_by_key[key2]
                        except KeyError:
                            pass
                    #elif sorted(key.split()) == sorted(key2.split()):
                    #    del input_dict[key2]
                    else:
                        if set(key2.split()).issubset(set(key.split())):
                            try:
                                del sorted_dict_by_key[key2]
                            except KeyError:
                                pass
            return sorted_dict_by_key

        def remove_dict_if_only_1_key(input_dict):
            if len(input_dict)==1:
                return(list(input_dict)[0])
            else:
                return(input_dict)

        sample_breeds['breed_found_counts']=sample_breeds['breed_found_counts'].apply(remove_duplicates)
        sample_breeds['breed_found_counts']=sample_breeds['breed_found_counts'].apply(remove_dict_if_only_1_key)
        sample_breeds['inferred_breed']=sample_breeds['breed'].where(~sample_breeds['breed'].isna(), np.nan)
        return(sample_breeds)
