Running MapReduce:
-------------------
hadoop jar <Jar location>/mapreduce2x-titanic-avgAge-job-0.0.1-SNAPSHOT-job.jar <hdfs location>/TitanicData.txt <hdfs output location>

Checking MapReduce Logs:
------------------------
yarn logs -applicationId <mapreduce applicationId>