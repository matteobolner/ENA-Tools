import pandas as pd
from tqdm import tqdm
from multiprocessing.pool import Pool
import requests
import numpy as np


def get_biosample_entry_from_ena_id(sample_accession):
    biosamples_server = "https://www.ebi.ac.uk/biosamples/samples/"
    r = requests.get(
        biosamples_server + sample_accession, headers={"Content-Type": "application/json"}
    )
    if r.ok:
        decoded = r.json()
        decoded = pd.json_normalize(decoded).transpose().to_dict()[0]
        decoded = pd.Series(decoded)
        decoded.index = [i.replace("characteristics.", "") for i in decoded.index]
        decoded=decoded.dropna()
        decoded=decoded.rename(index={'accession':'sample_accession'})
        return decoded
    else:
        return {"sample_accession":sample_accession}


def clean_biosamples_entry(sample_row):
    acceptables_list = [str, bool, float, int, np.int64, np.bool_, np.float64]
    newrow = pd.Series()
    for index, cell in sample_row.items():
        if index in newrow.index:
            index=index+"_2"
        cell_type = type(cell)
        if isinstance(cell, list):
            if len(cell) == 1:
                try:
                    newrow[index]=cell[0]["text"]
                except:
                    pass
        elif cell_type in acceptables_list:
            newrow[index]=cell
        else:
            raise TypeError(f"Cell type: {cell_type}")

    return newrow

def biosample_with_progress(sample_accession):
    biosample = get_biosample_entry_from_ena_id(sample_accession)
    biosample_clean = clean_biosamples_entry(biosample)
    #pbar.update(1)
    return biosample_clean

def build_biosamples_db(input, thread_number=4):
    if isinstance(input, list):
        sample_accessions = set(input)
    elif isinstance(input, pd.DataFrame):
        sample_accessions=set(input['sample_accession'])
    else:
        raise Exception("Wrong format for biosamples IDs: needs list or pandas Dataframe with column sample_accession")

    print("Building biosamples db from ENA samples...")

    with Pool(
        processes=thread_number,
    ) as p:
        results = list(
            tqdm(
                p.imap(biosample_with_progress, sample_accessions),
                total=len(sample_accessions),
            ))

    #pbar = tqdm(total=len(sample_accessions))
    #pool = ThreadPool(thread_number)
    #results = pool.map(biosample_with_progress, sample_accessions)
    #pool.close()
    #pool.join()
    biosamples_db = pd.concat(results, axis=1).transpose()
    print(f"Biosamples db completed (n={len(biosamples_db)})")

    return biosamples_db
