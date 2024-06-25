import pandas as pd
from multiprocessing.pool import ThreadPool
from tqdm import tqdm
from utilities import *
from multiprocessing.pool import ThreadPool
import numpy as np

def get_fao_breeds_dataset(species):
    fao_dataset=pd.read_csv(f"https://raw.githubusercontent.com/matteobolner/livestock_breeds_database/main/species/{species}/all_breeds.csv")
    fao_dataset['breed_no_diacritics']=fao_dataset['Breed/Most common name'].dropna().apply(remove_diacritics)
    fao_dataset['breed_letters_only']=fao_dataset['Breed/Most common name'].apply(strip_string_down_to_letters)
    fao_dataset=fao_dataset[fao_dataset['Breed/Most common name'].apply(len)>=3]
    return fao_dataset

def get_fao_breeds_stripped_list(species):
    fao_breeds_list=get_fao_breeds_dataset(species)['breed_letters_only'].unique().tolist()
    return fao_breeds_list

def count_breed_occurrences_in_string(sample, breed_list, input_string):
    counts = {substring: len(re.findall(substring, input_string)) for substring in breed_list}
    counts = {k:v for k,v in counts.items() if v!=0}
    return counts

def annotate_sample_with_breed(breed_list,biosamples,thread_number=4):
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
    biosamples_stripped=biosamples.set_index('sample_accession').apply(lambda row: row.dropna().astype(str).str.cat(sep=' '), axis=1).apply(strip_string_down_to_letters)

    def annotate_sample_breed(sample_stripped_info):
        pbar.update(1)
        id=sample_stripped_info[0]
        info=sample_stripped_info[1]
        counts={id:count_breed_occurrences_in_string(sample=id, breed_list=breed_list, input_string=info)}
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
