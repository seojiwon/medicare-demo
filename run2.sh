
hadoop fs -rm -r medicare/graph-count
hadoop fs -rm -r medicare/graph-sqrtcount
hadoop fs -rm -r medicare/graph-submitted
hadoop fs -rm -r medicare/graph-paid

pig -Dmapreduce.job.queue.name=data-science pre-process/similarity.pig

hadoop fs -getmerge medicare/graph-count data/graph-count.txt
hadoop fs -getmerge medicare/graph-sqrtcount data/graph-sqrtcount.txt
hadoop fs -getmerge medicare/graph-submitted data/graph-submitted.txt
hadoop fs -getmerge medicare/graph-paid data/graph-paid.txt
