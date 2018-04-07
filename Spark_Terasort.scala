
import org.apache.spark.{SparkConf, SparkContext}
import scala.runtime

/**
  * This class implemented the TeraSort using Spark
  *
  * Reference: https://github.com/ehiggs/spark-terasort/blob/master/src/main/scala/com/github/ehiggs/spark/terasort/TeraSort.scala
  * https://stackoverflow.com/questions/26347229/where-i-can-find-apache-spark-terasort-source
  *
  * This is the version for wrapping as programm, running with spark-submit
  *
  * This is version for directly running as scala script, running with spark-shell
  */



var TXTLENGTH = 10    //define the content length for sorting

    //spark configuration
val sparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setAppName(s"Spark_TeraSort")
val sc = new SparkContext(sparkConf)

//define the input and output file path and name
val inputfile = "/sortdata/sorttxt"
val outputfile = "/sortdata/output/finished_sort"

//split the line into <key, value> pair
val sortfile = sc.textFile(inputfile).map(line => (line.take(TXTLENGTH), line.drop(TXTLENGTH)))

//start sorting time
val starttime = System.currentTimeMillis()

//RDD sort based on the key value
val sort = sortfile.sortByKey()
//RDD mapping from <key, value> pair back to one line string
val pair2lines = sort.map(case (key, value) => s"$key $val")

//write back to disk file
pair2lines.saveAsTextFile(outputfile)

val endtime = System.currentTimeMillis()

println("Sorting totally takes: " + (endtime - starttime)/1000 + "s")



