
# prepare data for processing on Hadoop
python pre-process/prep_data.py
hadoop fs -rm -r medicare/raw_data
hadoop fs -put data/raw_data.txt medicare/raw_data

# Run pre-process to compute similarity and generate graph structure using PIG
pig pre-process/gen_graph.pig

# Copy output file to local disk to be used by socialite
hadoop fs -getmerge medicare/npi-mapping data/npi-mapping.txt
hadoop fs -getmerge medicare/specialty data/specialty.txt
hadoop fs -getmerge medicare/hcpcs-code data/hcpcs-code.txt
hadoop fs -getmerge medicare/npi-cpt-code data/npi-cpt-code.txt
hadoop fs -getmerge medicare/graph data/graph.txt
