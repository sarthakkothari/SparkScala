package ks

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import java.io._

import org.apache.spark.rdd.RDD

object TwitterRDDKS {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    
    val k1 = 1
    val k2 = 2
    
    
    val conf = new SparkConf().setAppName("AppName")
    val sc = new SparkContext(conf)
	
	def parseline( l : Array[String]): (Int, List[Int]) = { // function to return tuple (Int, List(Int)) from list of 2 string for adjlist
		(l(0).toInt, List(l(1).toInt))
	}

	def parseforSource( id : Int) : (Int, (Double, Double)) = { // setting Positive infinity as distance if not source
		if (id == k1)
			return (id, (0.0, Double.PositiveInfinity))
		else if (id == k2)
			return (id, (Double.PositiveInfinity, 0.0))
		
		(id, (Double.PositiveInfinity, Double.PositiveInfinity))
	
	}
	
	def parseInf( id: Int, dis : (Double, Double)) : (Int, (Double, Double)) = { // replacing inf to -1 to get max later
		var d1 = 0.0
		var d2 = 0.0
		if (dis._1 == Double.PositiveInfinity)
				d1 = -1
		else
				d1 = dis._1
		if (dis._2 == Double.PositiveInfinity)
				d2 = -1
		else
				d2 = dis._2
		
		(id, (d1, d2))
		
	}
	
	def extractVertex( id: Int, adj : List[Int], dis : (Double, Double)) : List[(Int, (Double, Double))] = { // adding 1 to all elements of adj list of the element
		adj.map(( _ , (dis._1 + 1, dis._2 + 1))) ++ List((id, dis)) 
	}

	def partitionedJoin(adjList: RDD[(Int, List[Int])], distances: RDD[(Int, (Double, Double))]) = { // create custom partitioner for adjlist
		val adjPart = adjList.partitioner match {
				case (Some(p)) => p
				case (None) => new HashPartitioner(adjList.partitions.length)
		}

		adjList.join(distances) //return join of adjlist and distance, RDD(Int, (List(int), (Double, Double)))
	}
	
	val textFile = sc.textFile(args(0)) // read textfile
	val adjList = textFile.map(line => parseline(line.split(","))).reduceByKey((a,b) => List.concat(a,b)) // create adjlist
	
	adjList.persist()
	
	var  distances = adjList.map(x => parseforSource(x._1)) // get RDD (Int ,(D1, D2)) where Int is nodeID, d1 is distance from source 1 and d2 is distance from source2
	for (i <- 1 to 10)
	{
		distances = partitionedJoin(adjList,distances) // join the dataset
				.flatMap(x => extractVertex(x._1, x._2._1, x._2._2)) // return every element in nodes's adj list with distance as (d1 + 1) and (d2+1)
				.reduceByKey((x,y) => (math.min(x._1,y._1), math.min(x._2,y._2))) // take the min for every ID
				//next Iteration
	}
	
	val maxDistance = distances.map(x => parseInf(x._1, x._2))
						.flatMap(x => List( ("D1", x._2._1), ("D2", x._2._2))).reduceByKey((x,y) => (math.max(x,y))) // replace inf as -1
						// map it to list of D1 with d1s and d2 as d2 and take the maximum

	maxDistance.saveAsTextFile(args(1)) // save max distance to text file()
	
	

  }
}



