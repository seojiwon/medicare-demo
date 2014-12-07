raw = load 'medicare/raw_data' using PigStorage('\t') as (
		npi: chararray,
		nppes_provider_last_org_name, nppes_provider_first_name, nppes_provider_mi, nppes_credentials, nppes_provider_gender, 
		nppes_entity_code, nppes_provider_street1, nppes_provider_street2, nppes_provider_city, nppes_provider_zip, nppes_provider_state,
		nppes_provider_country, provider_type, medicare_participation_indicator, place_of_service, 
		hcpcs_code: chararray, hcpcs_description: chararray, line_srvc_cnt: int, 
		bene_unique_cnt: int, bene_day_srvc_cnt: int, 
		average_Medicare_allowed_amt: float, stdev_Medicare_allowed_amt, 
		average_submitted_chrg_amt: float, stdev_submitted_chrg_amt, 
		average_Medicare_payment_amt: float, stdev_Medicare_payment_amt);
data = foreach raw generate npi, provider_type as specialty, hcpcs_code as cpt, hcpcs_description as cpt_desc, 
			    bene_day_srvc_cnt as count, average_submitted_chrg_amt as submitted, average_Medicare_payment_amt as paid;

sp1 = foreach data generate specialty;
sp2 = filter sp1 by specialty != '';
sp3 = distinct sp2;
sp4 = rank sp3;
specialties = foreach sp4 generate $1 as sp_name, (int)$0-1 as sp_index;

cd1 = foreach data generate cpt, cpt_desc;
cd2 = filter cd1 by cpt != '';
cd3 = distinct cd2;
cd4 = rank cd3;
cpts = foreach cd4 generate $1 as cpt_name, $2 as cpt_desc, (int)$0-1 as cpt_index;

np1 = foreach data generate npi;
np2 = filter np1 by (npi != '');
np3 = distinct np2;
np4 = rank np3;
npis = foreach np4 generate $1 as npi, (int)$0-1 as npi_index;

rmf medicare/specialty
rmf medicare/hcpcs-code
rmf medicare/npi-mapping
store specialties into 'medicare/specialty' using PigStorage('\t');
store cpts into 'medicare/hcpcs-code' using PigStorage('\t');
store npis into 'medicare/npi-mapping' using PigStorage('\t');

t1 = join data by npi, npis by npi;
t2 = join t1 by specialty, specialties by sp_name;
t3 = join t2 by cpt, cpts by cpt_name;
t4 = foreach t3 generate npi_index, sp_index, cpt_index, count, submitted, paid;
t5 = group t4 by (npi_index, sp_index, cpt_index);

t6 = foreach t5 generate group.npi_index as npi, group.sp_index as specialty, group.cpt_index as cpt, SUM($1.count) as count, SUM($1.submitted) as submitted, SUM($1.paid) as paid;
rmf medicare/npi-cpt-code-inx
store t6 into 'medicare/npi-cpt-code-inx' using PigStorage('\t'); 

t7 = group data by (npi, specialty, cpt);
t8 = foreach t7 generate group.npi as npi, group.specialty as specialty, group.cpt as cpt, SUM($1.count) as count, SUM($1.submitted) as submitted, SUM($1.paid) as paid;
rmf medicare/npi-cpt-code
store t8 into 'medicare/npi-cpt-code' using PigStorage('\t');

