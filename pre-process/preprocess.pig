RAW = load 'medicare/raw_data' using PigStorage('\t') as (
		npi: chararray,
		nppes_provider_last_org_name, nppes_provider_first_name, nppes_provider_mi, nppes_credentials, nppes_provider_gender, 
		nppes_entity_code, nppes_provider_street1, nppes_provider_street2, nppes_provider_city, nppes_provider_zip, nppes_provider_state,
		nppes_provider_country, provider_type, medicare_participation_indicator, place_of_service, 
		hcpcs_code: chararray, hcpcs_description: chararray, line_srvc_cnt: int, 
		bene_unique_cnt: int, bene_day_srvc_cnt: int, 
		average_Medicare_allowed_amt: float, stdev_Medicare_allowed_amt, 
		average_submitted_chrg_amt: float, stdev_submitted_chrg_amt, 
		average_Medicare_payment_amt: float, stdev_Medicare_payment_amt);
DATA = foreach RAW generate npi, provider_type as specialty, hcpcs_code as cpt, hcpcs_description as cpt_desc, bene_day_srvc_cnt as count;

SP1 = foreach DATA generate specialty;
SP2 = filter SP1 by specialty != '';
SP3 = distinct SP2;
SP4 = rank SP3;
SPECIALTY = foreach SP4 generate $1 as sp_name, (int)$0-1 as sp_index;
rmf medicare/specialty
store SPECIALTY into 'medicare/specialty' using PigStorage('\t');

CPT1 = foreach DATA generate cpt, cpt_desc;
CPT2 = filter CPT1 by cpt != '';
CPT3 = distinct CPT1;
CPT4 = rank CPT3;
HCPCS = foreach CPT4 generate $1 as cpt_name, $2 as cpt_desc, (int)$0-1 as cpt_index;
rmf medicare/hcpcs-code
store HCPCS into 'medicare/hcpcs-code' using PigStorage('\t');

NPI1 = foreach DATA generate npi;
NPI2 = filter NPI1 by (npi != '');
NPI3 = distinct NPI2;
NPI4 = rank NPI3;
NPI_MAPPING = foreach NPI4 generate $1 as npi, (int)$0-1 as npi_index;
rmf medicare/npi-mapping
store NPI_MAPPING into 'medicare/npi-mapping' using PigStorage('\t');

DATA1 = join DATA by npi, NPI_MAPPING by npi;
DATA2 = join DATA1 by specialty, SPECIALTY by sp_name;
DATA3 = join DATA2 by cpt, HCPCS by cpt_name;
DATA4 = foreach DATA3 generate npi_index, sp_index, cpt_index, count;
DATA_GRP = group DATA4 by (npi_index, sp_index, cpt_index);
NPI_CPT_CODE = foreach DATA_GRP generate group.npi_index as npi, group.sp_index as specialty, group.cpt_index as cpt, SUM($1.count) as count;
rmf medicare/npi-cpt-code
store NPI_CPT_CODE into 'medicare/npi-cpt-code' using PigStorage('\t'); 

