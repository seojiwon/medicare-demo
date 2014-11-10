SET  pig.tmpfilecompression true
SET  pig.tmpfilecompression.codec gzip
SET  default_parallel 8 
SET  mapreduce.map.memory.mb 7000
SET  mapreduce.reduce.memory.mb 7000
SET  mapreduce.map.java.opts -Xmx6g
SET  mapreduce.reduce.java.opts -Xmx6g

REGISTER pre-process/datafu-pig-1.3.0-SNAPSHOT-jarjar.jar; 
define LSH datafu.pig.hash.lsh.CosineDistanceHash('5949', '5000', '15', '0');

REGISTER pre-process/medicare-udf/target/medicare-udf-1.0-SNAPSHOT.jar;
define SimUDF com.hortonworks.demo.SimUDF(); 

inp = load 'medicare/npi-cpt-code-inx' using PigStorage('\t') as (npi: chararray, sp_inx: chararray, code_inx: chararray, count: double, submitted: double, paid: double);

d1 = group inp by code_inx;
d2 = foreach d1 generate group as code_inx, COUNT($1) as cpt_unique, SUM($1.count) as cpt_total;
d3 = filter d2 by cpt_total <= 10000000;	-- Only keep CPTs where total count < 10M
d4 = group inp by npi;
d5 = foreach d4 generate group as npi, COUNT($1) as npi_count;
d6 = filter d5 by npi_count >= 3;
d7 = join inp by code_inx, d3 by code_inx;
d8 = foreach d7 generate npi, (int)inp::code_inx as code_inx, count, submitted, paid;
d9 = join d8 by npi, d6 by npi;
data = foreach d9 generate d6::npi as npi, (int)code_inx as inx, count, submitted, paid;

DEFINE gen_graph(data_in) returns out {
  grp = group $data_in by npi;
  pts = foreach grp generate group as npi, $data_in.(inx,val) as cpt_vec;
  pts_hashed = foreach pts generate npi, cpt_vec, FLATTEN(LSH(cpt_vec)) as (hash_id:int, hash_val:long);
  partitions = group pts_hashed by (hash_id, hash_val);
  pts_with_partitions = join pts_hashed by (hash_id, hash_val), partitions by (group.$0, group.$1);
  res = foreach pts_with_partitions generate npi as npi1, FLATTEN(SimUDF(cpt_vec, 0.25, pts_hashed)) as npi2;
	$out = filter res by npi1 > npi2;
};

-- Generate graph with counts
v_cnt = foreach data generate npi, inx, (double)count as val;
out = gen_graph(v_cnt);
store out into 'medicare/graph-count' using PigStorage('\t');
exec;

-- Generate graph with sqrt counts
v_sqrt = foreach data generate npi, inx, SQRT(count) as val;
out = gen_graph(v_sqrt);
store out into 'medicare/graph-sqrtcount' using PigStorage('\t');
exec;

-- Generate graph with submitted amount
v_sub = foreach data generate npi, inx, (double)submitted as val;
out = gen_graph(v_sub);
store out into 'medicare/graph-submitted' using PigStorage('\t');
exec;

-- Generate graph with paid amount
v_paid = foreach data generate npi, inx, (double)paid as val;
out = gen_graph(v_paid);
store out into 'medicare/graph-paid' using PigStorage('\t');

