package tc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level


object TwitterJoinRS {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Edges Count")
    val sc = new SparkContext(conf)
    val max = 30000

    def parseline( l : Array[String]): (Int, Int) = { // function to return tuple (Int, Int) from list of 2 string
      (l(0).toInt, l(1).toInt)
    }

    
    val textFile = sc.textFile(args(0))
    val filter_l = textFile.map(line => parseline(line.split(","))) // get Tuple(Int, Int) (From, To)
                      .filter({case (k,v) =>  k <= max && v <= max}) // Filter
    val filter_r = textFile.map(line => parseline(line.split(","))) // get Tuple(Int, Int) (From, To)
                    .filter({case (k,v) =>  k <= max && v <= max}) // Filter
                    .map( k => (k._2, k._1)) // Flip

    val joinedRDD = filter_l.join(filter_r) // join on keys
                      .map(x => (x._2._1 , x._2._2)) // get From and to nodes (Key is to middle node)
                      .filter({case (k,v) => k != v}) // Filter cycle
                      .join(filter_l) // join with original edges
                      .map(x => (x._2._1, x._2._2)) // remove intermediate values 
                      .filter({case (k,v) => k == v}) // check if values are same
    	
    val res = joinedRDD.count // store count
          
    val ans = sc.parallelize(Seq(res))
    ans.saveAsTextFile(args(1))
  
  }
}