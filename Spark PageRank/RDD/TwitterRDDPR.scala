package pr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.log4j.LogManager
import org.apache.log4j.Level

import java.io._

object TwitterRDDPR {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }

		val k = 100
		val k2 = (math.pow(k,2)).toInt
		var l = List[(Int, Int)]()
		var l1 = List[(Int, Double)]()

		val conf = new SparkConf().setAppName("AppName")
		val sc = new SparkContext(conf)
		val spark = SparkSession.builder().appName("AppName")
			.config("spark.master", "local").getOrCreate() //Create spark object
l

		for(i <- 1 to k2) { // creating adj list
			if ((i % k) != 0) {
				l =  List.concat(l, List((i, i+1)))
			}
			else
				l = List.concat(l, List((i,0)))
			l1 = List.concat(l1, List((i,1.0/k2))) // creating initial pageranks
		}

		val Graph = sc.parallelize(l) // make GraphRDD
		var prTable = sc.parallelize(l1) // make PR rank RDD

		def addVals(i: Int, d: Double, d1: Double) : (Int, Double) = { // adding delta to pr
			if (i == 0)
				return (i, d)
			else
				return (i, d+d1)
		}

	
	Graph.persist()

	for (i <- 1 to 10)
	{
		prTable = Graph.join(prTable)
										.flatMap(joinPair => if (joinPair._1 % k == 1)
														List((joinPair._1, 0), joinPair._2)
															else
															List(joinPair._2)) // join table and assgin pagerank, start nodes get 0
		prTable = prTable.reduceByKey(_+_) // calculate the total page rank
		var dangling = (prTable.lookup(0)(0)) / k2 //calculate delta

		prTable = prTable.map(x => addVals(x._1, x._2, dangling)) // add delta to pr.

		var sumMass = prTable.map( x => if (x._1 == 0)  0 else x._2).sum() //calculate total pr pass except dangling

		print("****************************************************************")
		print("SumMassOf" + i + "=" + sumMass) // print
		print("****************************************************************")

	}

	System.out.println(prTable.toDebugString)

	var max = prTable.sortByKey(true).take(101) //sort by ID and get top 101

	var ans = sc.parallelize(max) // convert array to RDD
	ans.saveAsTextFile(args(1)) // save to text
	
	

  }


}



