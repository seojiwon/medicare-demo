
hadoop fs -rm -r medicare/graph
pig  similarity.pig
hadoop fs -getmerge medicare/graph data/graph.txt
