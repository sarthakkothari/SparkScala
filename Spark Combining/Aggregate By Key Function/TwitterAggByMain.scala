package tc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object TwitterAggByMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Edges Count")
    val sc = new SparkContext(conf)

    
    val textFile = sc.textFile(args(0))
    val init = 0
    val addCount = (c1: Int, c2: Int) => c1 + c2 // add count per user per partition
    val mergeCount = (m1: Int, m2: Int) => m1 + m2 // combine counts across partition
    val counts = textFile.map(line => line.split(",")) // Using map instead of flat map to not loose the schema for edges
                .map(word => (word(1), 1)) // Using second element of the array to know the follower count
                .aggregateByKey(init)(addCount,mergeCount) // initialize accumulator per key at 0 and pass functions for
                // adding values to count and merging across user
              

	  print("*********************************************\n*********************\n***********")
	  print(""+counts.toDebugString)
	  print("*********************************************\n*********************\n***********")
    counts.saveAsTextFile(args(1))
  }
}