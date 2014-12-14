SET  pig.tmpfilecompression true
SET  pig.tmpfilecompression.codec gzip
SET  pig.cachedbag.memusage 0.8

register 'pre-process/udfs.py' using org.apache.pig.scripting.jython.JythonScriptEngine as udfs;

INP = load 'medicare/npi-cpt-code' using PigStorage('\t') 
				as (npi: chararray, sp_inx: chararray, code_inx: chararray, count: double);

-- Filter out noisy CPT and NPI codes
CODE_GRP = group INP by code_inx;
CNT_PER_CODE = foreach CODE_GRP generate group as code_inx, SUM($1.count) as cpt_total;
VALID_CODES = filter CNT_PER_CODE by cpt_total <= 10000000;	-- Only keep CPTs where total count <= 10M
INP_JOINED = join INP by code_inx, VALID_CODES by code_inx;
INP_WITH_VALID_CODES = foreach INP_JOINED generate npi, INP::code_inx as code_inx, count;
NPI_GRP = group INP_WITH_VALID_CODES by npi;
CNT_PER_NPI = foreach NPI_GRP generate group as npi, COUNT($1) as npi_count;
VALID_NPIS = filter CNT_PER_NPI by npi_count >= 3;
INP_JOINED2 = join INP_WITH_VALID_CODES by npi, VALID_NPIS by npi;
DATA = foreach INP_JOINED2 generate VALID_NPIS::npi as npi, (int)INP_WITH_VALID_CODES::code_inx as cpt_inx, count;

-- Generate graph with counts
GRP1 = group DATA by npi parallel 40;
PTS = foreach GRP1 generate group as npi, DATA.(cpt_inx, count) as cpt_vec;
PTS_TOP = foreach PTS generate npi, cpt_vec, FLATTEN(udfs.top_cpt(cpt_vec)) as (cpt_inx: int, count: double);
PTS_TOP_CPT = foreach PTS_TOP generate npi, cpt_vec, cpt_inx;
CPT_GRP = group PTS_TOP_CPT by cpt_inx parallel 40;
NPI_CLUST = foreach CPT_GRP generate PTS_TOP_CPT.(npi, cpt_vec) as npi_bag;
NPI_AND_CLUST = foreach NPI_CLUST generate FLATTEN(npi_bag) as (npi: chararray, cpt_vec), npi_bag;
OUT = foreach NPI_AND_CLUST generate npi as npi1, FLATTEN(udfs.similarNpi(npi, cpt_vec, npi_bag, 0.75)) as npi2;

rmf medicare/graph
store OUT into 'medicare/graph' using PigStorage('\t');

