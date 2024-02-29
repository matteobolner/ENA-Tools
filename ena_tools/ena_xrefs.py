import requests
import pandas as pd


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
        decoded=pd.DataFrame(decoded)
        return decoded
    else:
        return np.nan

def build_study_xrefs_db(self, input, thread_number=4):
    if isinstance(input, list):
        study_ids = set(input)
    elif isinstance(input, pd.DataFrame):
        study_ids=set(input['study_accession'])
    else:
        raise Exception("Wrong format for Xrefs IDs: needs list or pandas Dataframe with column study_accession")

    pbar = tqdm(total=len(study_ids))
    study_ids=set(study_ids)
    pool = ThreadPool(thread_number)
    xref_list = pool.map(get_xref, study_ids)
    pool.close()
    pool.join()
    xrefs_df=pd.concat(xref_list)
    return xrefs_df
