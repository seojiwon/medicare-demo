SET  pig.tmpfilecompression true
SET  pig.tmpfilecompression.codec gzip
SET  pig.cachedbag.memusage 0.8
SET  mapreduce.map.memory.mb 5100
SET  mapreduce.reduce.memory.mb 5100
SET mapreduce.map.java.opts -Xmx4g
SET mapreduce.reduce.java.opts -Xmx4g

register 'pre-process/udfs.py' using org.apache.pig.scripting.jython.JythonScriptEngine as udfs;

inp = load 'medicare/npi-cpt-code-inx' using PigStorage('\t') as (npi: chararray, sp_inx: chararray, code_inx: chararray, count: double, submitted: double, paid: double);

d1 = group inp by code_inx;
d2 = foreach d1 generate group as code_inx, COUNT($1) as cpt_unique, SUM($1.count) as cpt_total;
d3 = filter d2 by cpt_total <= 10000000;	-- Only keep CPTs where total count <= 10M
d4 = join inp by code_inx, d3 by code_inx;
d5 = foreach d4 generate npi, inp::code_inx as code_inx, count, submitted, paid;
d6 = group d5 by npi;
d7 = foreach d6 generate group as npi, COUNT($1) as npi_count;
d8 = filter d7 by npi_count >= 3;
d9 = join d5 by npi, d8 by npi;
data = foreach d9 generate d8::npi as npi, (int)d5::code_inx as cpt_inx, count, submitted, paid;

DEFINE gen_graph(data_in) returns out {
	grp1 = group $data_in by npi parallel 40;
	pts = foreach grp1 generate group as npi, $data_in.(cpt_inx,val) as cpt_vec;
	pts_top = foreach pts generate npi, cpt_vec, FLATTEN(udfs.top_cpt(cpt_vec)) as (cpt_inx: int, val: double);
	p1 = foreach pts_top generate npi, cpt_vec, cpt_inx;
	p2 = group p1 by cpt_inx parallel 40;
	p3 = foreach p2 generate p1.(npi, cpt_vec) as npi_bag;
	p4 = foreach p3 generate FLATTEN(npi_bag) as (npi: chararray, cpt_vec), npi_bag;
	$out = foreach p4 generate npi as npi1, FLATTEN(udfs.similarNpi(npi, cpt_vec, npi_bag, 0.75)) as npi2;
};

-- Generate graph with counts
v_cnt = foreach data generate npi, cpt_inx, (double)count as val;
out = gen_graph(v_cnt);
store out into 'medicare/graph-count' using PigStorage('\t');
exec;

-- Generate graph with sqrt counts
v_sqrt = foreach data generate npi, cpt_inx, SQRT(count) as val;
out = gen_graph(v_sqrt);
store out into 'medicare/graph-sqrtcount' using PigStorage('\t');
exec;

-- Generate graph with submitted amount
v_sub = foreach data generate npi, cpt_inx, (double)submitted as val;
out = gen_graph(v_sub);
store out into 'medicare/graph-submitted' using PigStorage('\t');
exec;

-- Generate graph with paid amount
v_paid = foreach data generate npi, cpt_inx, (double)paid as val;
out = gen_graph(v_paid);
store out into 'medicare/graph-paid' using PigStorage('\t');

