SET  pig.tmpfilecompression true
SET  pig.tmpfilecompression.codec gzip
SET  default_parallel 8
SET  mapreduce.reduce.memory.mb 2048

REGISTER datafu-pig-1.3.0-SNAPSHOT-jarjar.jar; 
define UnorderedPairs datafu.pig.bags.UnorderedPairs();
define LSH datafu.pig.hash.lsh.CosineDistanceHash('5949', '500', '5', '0');

REGISTER pre-process/medicare-udf/target/medicare-udf-1.0-SNAPSHOT.jar;
define SimUDF com.hortonworks.demo.SimUDF(); 

inp = load 'medicare/npi-cpt-code-inx' using PigStorage('\t') as (npi: chararray, sp_inx: chararray, code_inx: chararray, count: float);

d1 = group inp by code_inx;
d2 = foreach d1 generate group as code_inx, COUNT($1) as cpt_count;
d3 = filter d2 by cpt_count <= 50000;
d4 = group inp by npi;
d5 = foreach d4 generate group as npi, COUNT($1) as npi_count;
d6 = filter d5 by npi_count >= 3;
d7 = join inp by code_inx, d3 by code_inx;
d8 = foreach d7 generate npi, (int)inp::code_inx as code_inx, count;
d9 = join d8 by npi, d6 by npi;
data = foreach d9 generate d6::npi as npi, code_inx, count;

a1 = foreach data generate npi, (int)code_inx as inx, count;
a2 = group a1 by npi;
a3 = foreach a2 generate group as npi, a1.(inx,count) as cpt_vec;

a4 = foreach a3 generate npi, cpt_vec, FLATTEN(LSH(cpt_vec)) as (hash_id:int, hash_val:long);
a5 = group a4 by (hash_id, hash_val);
a6 = foreach a5 generate FLATTEN(SimUDF(a4.(npi, cpt_vec)));

store a6 into 'medicare/graph' using PigStorage('\t');



