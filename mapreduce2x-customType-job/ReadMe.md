Running MapReduce:
-------------------
hadoop jar <Jar location>/mapreduce2x-customType-job-0.0.1-SNAPSHOT-job.jar <hdfs location>/Players.txt <hdfs output location>

Checking MapReduce Logs:
------------------------
yarn logs -applicationId <mapreduce applicationId>
