RAW = load 'medicare/raw_data' using PigStorage('\t') as (
		npi: chararray,
		cms_specialty:chararray, classification:chararray, specialization: chararray,
		hcpcs_code: chararray, hcpcs_description: chararray, bene_day_srvc_cnt: int);
DATA = foreach RAW generate npi, cms_specialty, classification, specialization, hcpcs_code as cpt, hcpcs_description as cpt_desc, bene_day_srvc_cnt as count;

SP1 = foreach DATA generate classification as specialty;
SP2 = filter SP1 by specialty != '';
SP3 = distinct SP2;
SP4 = rank SP3;
SPECIALTY = foreach SP4 generate $1 as sp_name, (int)$0-1 as sp_index;
rmf medicare/specialty
store SPECIALTY into 'medicare/specialty' using PigStorage('\t');

CPT1 = foreach DATA generate cpt, cpt_desc;
CPT2 = filter CPT1 by cpt != '';
CPT3 = distinct CPT2;
CPT4 = rank CPT3;
HCPCS = foreach CPT4 generate $1 as cpt_code, $2 as cpt_desc, (int)$0-1 as cpt_index;
rmf medicare/hcpcs-code
store HCPCS into 'medicare/hcpcs-code' using PigStorage('\t');

NPI1 = foreach DATA generate npi;
NPI2 = filter NPI1 by (npi != '');
NPI3 = distinct NPI2;
NPI4 = rank NPI3;
NPI_MAPPING = foreach NPI4 generate $1 as npi, (int)$0-1 as npi_index;
rmf medicare/npi-mapping
store NPI_MAPPING into 'medicare/npi-mapping' using PigStorage('\t');

DATA0 = filter DATA by NOT(cpt_desc MATCHES '.*[Oo]ffice/outpatient visit.*');	   -- remove 'too common' CPT for regular office visit
DATA1 = join DATA0 by npi, NPI_MAPPING by npi;
DATA2 = join DATA1 by classification, SPECIALTY by sp_name;
DATA3 = join DATA2 by cpt, HCPCS by cpt_code;
DATA4 = foreach DATA3 generate npi_index, sp_index, classification, specialization, cpt_index, count;
DATA_GRP = group DATA4 by (npi_index, sp_index, cpt_index);
NPI_CPT_CODE = foreach DATA_GRP generate group.npi_index as npi, 
																				 group.sp_index as specialty, MIN(DATA4.classification) as classification, MIN(DATA4.specialization) as specialization,
																				 group.cpt_index as cpt, SUM(DATA4.count) as count;
rmf medicare/npi-cpt-code
store NPI_CPT_CODE into 'medicare/npi-cpt-code' using PigStorage('\t'); 

