
# Tera Sort benchmarking


----

How to run the code:

1. SM_Terasort.java:
compile: 
```shell
javac SM_Terasort.java
```
run: 
```shell
java -Xms8192m -Xmx10240m SM_Terasort
```
note: here the size for xms and xmx means the size of initial heap size and maximum heap size for jvm. This should be determined according to the configure file. If you configure larger chunk size, then this number should be increased to make sure one chunk can be sucessfully loaded into memory.


2. Hadoop_Terasort.java:
compile: 
```shell
/home/ubuntu/hadoop/bin/hadoop com.sun.tools.javac.Main Hadoop_Terasort.java
jar cf Hadoop_Terasort.jar *.class
```	
run:
```shell
/home/ubuntu/hadoop/bin/hadoop jar Hadoop_Terasort.jar Hadoop_Terasort /sortdata/sorttxt /sortdata/output
```
	
3. Spark_Terasort.scala:
compile: directly run Spark Shell, no need to compile
		
run:
```shell
/home/ubuntu/spark/bin/spark-shell 
```





