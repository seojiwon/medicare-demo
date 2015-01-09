
# copy raw data file into HDFS without header files
python pre-process/getspec.py
hadoop fs -rm -r medicare/raw_data
hadoop fs -put data/raw_data.txt medicare/raw_data

# Run pre-process and similarity PIG scripts
pig pre-process/preprocess.pig
pig pre-process/similarity.pig

hadoop fs -getmerge medicare/npi-mapping data/npi-mapping.txt
hadoop fs -getmerge medicare/specialty data/specialty.txt
hadoop fs -getmerge medicare/hcpcs-code data/hcpcs-code.txt
hadoop fs -getmerge medicare/npi-cpt-code data/npi-cpt-code.txt
hadoop fs -getmerge medicare/graph data/graph.txt
hadoop fs -getmerge medicare/graph-sqrt data/graph-sqrt.txt
