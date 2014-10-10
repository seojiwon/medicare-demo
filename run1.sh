
hadoop fs -mkdir medicare
if hadoop fs -test -e medicare/raw_data; then
	echo "No need to copy file"
else
	cat data/Medicare-Physician-and-Other-Supplier-PUF-CY2012.txt | sed '1,2d' > raw_data.txt
	hadoop fs -put raw_data.txt medicare/raw_data
	rm raw_data.txt
fi

pig pre-process/preprocess.pig

hadoop fs -getmerge medicare/npi-mapping data/npi-mapping.txt
hadoop fs -getmerge medicare/specialty data/specialty.txt
hadoop fs -getmerge medicare/hcpcs-code data/hcpcs-code.txt
hadoop fs -getmerge medicare/npi-cpt-code-inx data/npi-cpt-code.txt
