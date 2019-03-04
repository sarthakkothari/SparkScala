package tc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level


object TwitterJoinREP {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Edges Count")
    val sc = new SparkContext(conf)
    val max = 52000

	def parseline( l : Array[String]): (Int, Int) = { // Return (Int, Int) tuple from list of 2 strings
		(l(0).toInt, l(1).toInt)
	}

    
    val textFile = sc.textFile(args(0)) // read file
    val filter_rdd = textFile.map(line => parseline(line.split(","))) // get (Int int) tuple from of edges
    					.filter({case (k,v) =>  k <= max && v <= max}) // filter edges
    					.groupByKey() // group by key
    					.collectAsMap() // collect so that it can be broadcasted
    					.map( x => (x._1, x._2.toList)) //create a hashmap with groupby function
    	
   	val filter_edges = textFile.map(line => parseline(line.split(",")))
    					   .filter({case (k,v) =>  k <= max && v <= max}) // filter edges
    	
    	
	val Hashmap = filter_edges.sparkContext.broadcast(filter_rdd) // boradcast variable

	def parseTuple(l : (Int, Int)) : (Long) = {
		var empty  = List[Int]() // create empty list
		val from = l._1
		val to = l._2
		var count = 0
		val myHash = Hashmap.value // get hashmap
		val goingTo = myHash.getOrElse(to, empty) // get adjacency from to
		var item = 0
		for(item <- goingTo) //iterate over adjaceny of to
		{
			var isFrom = 0
			val goingBack = myHash.getOrElse(item, empty) // get item's adjacency list
			for (isFrom <- goingBack)
			{
				if (isFrom == from) // check if it goes back and completes triangle
				{
					count = count + 1 // increase count
				}
			}

		}
		return count // return count
	}
	
	val res = filter_edges.mapPartitions(iter => iter.map(x => parseTuple(x))).sum() //parse tuple and get sum each key-no-of-triangle the list
    	
          
	val ans = sc.parallelize(Seq(res))
	ans.saveAsTextFile(args(1))
  }
}