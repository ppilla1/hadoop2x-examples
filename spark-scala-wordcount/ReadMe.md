# Ref. : http://spark.apache.org/docs/latest/submitting-applications.html

# Submitting Spark Job through windows command line
# Note :
#-------
# 1. Don't use double quotes anywhere
# 2. Spark Job jar file should be placed in same location where "spark-submit" command is executed

spark-submit --class <complete job classname with package name> <jar file> <param-1> .. 

#ex (Running in Local thread):
#-----------------------------
spark-submit --master local --class org.disknotfound.sparkjob.WordCount wordcount-spark-0.0.1-SNAPSHOT.jar debug.log
