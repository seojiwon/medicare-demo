
SET  pig.tmpfilecompression true
SET  pig.tmpfilecompression.codec gzip
SET  pig.cachedbag.memusage 0.8
SET  pig.exec.mapPartAgg true

register 'pre-process/udfs.py' using org.apache.pig.scripting.jython.JythonScriptEngine as udfs;

RAW = load 'medicare/raw_data' using PigStorage('\t') as (
		npi: chararray,
		cms_specialty:chararray, classification:chararray, specialization: chararray,
		hcpcs_code: chararray, hcpcs_description: chararray, bene_day_srvc_cnt: int);
DATA = foreach RAW generate npi, cms_specialty, classification, specialization, hcpcs_code as cpt, hcpcs_description as cpt_desc, bene_day_srvc_cnt as count;

-- generate speciality file using "classification" field (ignoring cms_specialty or "specialization")
SP1 = foreach DATA generate classification as specialty;
SP2 = filter SP1 by specialty != '';
SP3 = distinct SP2;
SP4 = rank SP3;
SPECIALTY = foreach SP4 generate $1 as sp_name, (int)$0-1 as sp_index;
rmf medicare/specialty
store SPECIALTY into 'medicare/specialty' using PigStorage('\t');

-- generate CPT file
CPT1 = foreach DATA generate cpt, cpt_desc;
CPT2 = filter CPT1 by cpt != '';
CPT3 = distinct CPT2;
CPT4 = rank CPT3;
HCPCS = foreach CPT4 generate $1 as cpt_code, $2 as cpt_desc, (int)$0-1 as cpt_index;
rmf medicare/hcpcs-code
store HCPCS into 'medicare/hcpcs-code' using PigStorage('\t');

-- generate NPI mapping file
NPI1 = foreach DATA generate npi;
NPI2 = filter NPI1 by (npi != '');
NPI3 = distinct NPI2;
NPI4 = rank NPI3;
NPI_MAPPING = foreach NPI4 generate $1 as npi, (int)$0-1 as npi_index;
rmf medicare/npi-mapping
store NPI_MAPPING into 'medicare/npi-mapping' using PigStorage('\t');

-- generate final dataset with npi, specialty, CPT and count
DATA0 = filter DATA by NOT(cpt_desc MATCHES '.*[Oo]ffice/outpatient visit.*');	   -- remove 'too common' CPT for regular office visit
DATA1 = join DATA0 by npi, NPI_MAPPING by npi using 'replicated';
DATA2 = join DATA1 by classification, SPECIALTY by sp_name using 'replicated';
DATA3 = join DATA2 by cpt, HCPCS by cpt_code using 'replicated';
DATA4 = foreach DATA3 generate npi_index, sp_index, cpt_index, count;
DATA_GRP = group DATA4 by (npi_index, sp_index, cpt_index) parallel 20;
NPI_CPT_CODE = foreach DATA_GRP generate group.npi_index as npi, 
																				 group.sp_index as specialty, 
																				 group.cpt_index as cpt_inx, (int)SUM(DATA4.count) as count;
rmf medicare/npi-cpt-code
store NPI_CPT_CODE into 'medicare/npi-cpt-code' using PigStorage('\t'); 

-- Filter out noisy CPT codes and noise NPIs
CODE_GRP = group NPI_CPT_CODE by cpt_inx parallel 5;
CNT_PER_CODE = foreach CODE_GRP generate group as cpt_inx, SUM($1.count) as cpt_total;
VALID_CODES = filter CNT_PER_CODE by cpt_total <= 10000000;	-- Only keep CPTs where total count <= 10M
JOINED = join NPI_CPT_CODE by cpt_inx, VALID_CODES by cpt_inx;
JOINED_WITH_VALID_CODES = foreach JOINED generate npi, NPI_CPT_CODE::cpt_inx as cpt_inx, count;
NPI_GRP = group JOINED_WITH_VALID_CODES by npi parallel 5;
CNT_PER_NPI = foreach NPI_GRP generate group as npi, COUNT($1) as npi_count;
VALID_NPIS = filter CNT_PER_NPI by npi_count >= 3;
JOINED2 = join JOINED_WITH_VALID_CODES by npi, VALID_NPIS by npi;
DATA = foreach JOINED2 generate VALID_NPIS::npi as npi, (int)JOINED_WITH_VALID_CODES::cpt_inx as cpt_inx, count;

SET  mapreduce.reduce.memory.mb 5120
SET  mapreduce.reduce.java.opts -Xmx4800m

-- Macro: generate graph with counts
DEFINE prep_graph(RAW_DATA) returns OUT {
 	GRP = group $RAW_DATA by npi parallel 10;
	PTS = foreach GRP generate group as npi, $RAW_DATA.(cpt_inx, count) as cpt_vec;
	PTS_TOP = foreach PTS generate npi, cpt_vec, FLATTEN(udfs.top_cpt(cpt_vec)) as (cpt_inx: int, count: int);
	PTS_TOP_CPT = foreach PTS_TOP generate npi, cpt_vec, cpt_inx;
	CPT_GRP = group PTS_TOP_CPT by cpt_inx parallel 10;
	CPT_CLUST = foreach CPT_GRP generate PTS_TOP_CPT.(npi, cpt_vec) as npi_bag;
	CPT_CLUST_WITH_ID = RANK CPT_CLUST;
  NPI_AND_BAGID = foreach CPT_CLUST_WITH_ID generate FLATTEN(npi_bag) as (npi: int, cpt_vec), $0 as bag_id;
	NPI_AND_BAGID_SHUF = foreach (GROUP NPI_AND_BAGID by npi parallel 40) generate FLATTEN($1) as (npi:int, cpt_vec, bag_id);
 	NPI_AND_CLUST = join NPI_AND_BAGID_SHUF by bag_id, CPT_CLUST_WITH_ID by $0 using 'replicated';
	PAIRS = foreach NPI_AND_CLUST generate npi as npi1, FLATTEN(udfs.similarNpi(npi, cpt_vec, npi_bag, 0.75)) as npi2;
	$OUT = distinct PAIRS;
};

OUT = prep_graph(DATA);
rmf medicare/graph
store OUT into 'medicare/graph' using PigStorage('\t');

