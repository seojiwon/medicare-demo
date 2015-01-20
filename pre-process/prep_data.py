import pandas as pd

# Read npidata file
NPIDATA = pd.read_csv('ref_data/npidata_20050523-20141207.csv', sep=',', quotechar='"', quoting=1, header=0, dtype=str, usecols = ['NPI','Healthcare Provider Taxonomy Code_1'])

# Read NUCC provider taxonomy codes
TAXONOMY = pd.read_csv('ref_data/nucc_taxonomy_141.csv', sep=',', header=0)

# map NPI to taxonomy description levels (classification, specialization)
JOINED = pd.merge(NPIDATA, TAXONOMY, how='left', left_on = 'Healthcare Provider Taxonomy Code_1', right_on = 'Code');
MAPPING = JOINED[['NPI', 'Classification', 'Specialization']]
MAPPING.to_csv('ref_data/npi2specialty.txt', sep='\t', header=None, index=False)

# Add classification and specialization codes to providers in CMS medicare-B dataset
DATASET = pd.read_csv('ref_data/Medicare-Physician-and-Other-Supplier-PUF-CY2012.txt', sep='\t', header=0, skiprows=[1], dtype=str, usecols=['npi', 'provider_type', 'hcpcs_code', 'hcpcs_description', 'bene_day_srvc_cnt'])
JOINED2 = pd.merge(DATASET, MAPPING, how='left', left_on = 'npi', right_on = 'NPI');
RAW_DATA = JOINED2[['npi', 'provider_type', 'Classification', 'Specialization', 'hcpcs_code', 'hcpcs_description', 'bene_day_srvc_cnt']]

# Store resulting dataset, with only necessary fields and without header
RAW_DATA.to_csv('data/raw_data.txt', sep='\t', index=False, header=None)



