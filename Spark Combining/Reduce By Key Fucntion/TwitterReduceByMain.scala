package tc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object TwitterReduceByMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Edges Count")
    val sc = new SparkContext(conf)

    
    val textFile = sc.textFile(args(0))
    val counts = textFile.map(line => line.split(",")) // Using map instead of flat map to not loose the schema for edges
                .map(word => (word(1), 1)) // Using second element of the array to know the follower count
                .reduceByKey(_+_) // add counts per key basis
              

	  print("*********************************************\n*********************\n***********")
	  print(""+counts.toDebugString)
	  print("*********************************************\n*********************\n***********")
    counts.saveAsTextFile(args(1))
  }
}