package ks

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.log4j.LogManager
import org.apache.log4j.Level

import java.io._

object TwitterDatasetKS {
  
  def main(args: Array[String]) {
		val logger: org.apache.log4j.Logger = LogManager.getRootLogger
		if (args.length != 2) {
		logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
		System.exit(1)
		}
    
		val k1 = 1 // source1
		val k2 = 2 // soruce2
    
    
		val conf = new SparkConf().setAppName("AppName")
		val sc = new SparkContext(conf)
		val spark = SparkSession.builder().appName("AppName").config("spark.master", "local").getOrCreate() //Create spark object
		import spark.implicits._

		spark.conf.set("spark.sql.broadcastTimeout",  36000)
		var GraphDF = spark.read.csv(args(0)).toDF("From", "To").repartition($"From") //read the graph and create dataset
		
		GraphDF.persist()
	
		var distanceTable = GraphDF.select($"from").distinct()
							.select($"from".as("ID"),
									when($"from" === k1, 0.0).otherwise(Double.PositiveInfinity).as("d1"),
									when($"from" === k2, 0.0).otherwise(Double.PositiveInfinity).as("d2"))
						.repartition($"ID") // create distance table where id is node ID, d1 is distance to ID from k1 and d2 is distance to ID from k2
		
		for( j <- 1 to 10)
		{
		
			val tempdistance = GraphDF.as("G").join(distanceTable.as("D"), $"G.from" === $"D.ID") // join on Graph
									.select($"G.To".as("ID"), ($"D.d1"+1).as("d1"), ($"D.d2"+1).as("d2")) // select going to and add distance as previous+1 // 1 hop jump 
									.union(distanceTable) // union previous iteration hops
		
			distanceTable = tempdistance.groupBy($"ID").min("d1", "d2").toDF("ID", "d1", "d2") // compare if previous iteration was shorter

			// next iteration
		}
	
	
	
		val maxValues = distanceTable.select($"ID", when($"d1" === Double.PositiveInfinity, -1).otherwise($"d1").as("d1"), 
												when($"d2" === Double.PositiveInfinity, -1).otherwise($"d2").as("d2")).select(max($"d1"), max($"d2")) // set infinity to negative 
																																					//and then select max of d1 and d2

												
		System.out.println(maxValues.show())
		
		sc.stop()

  	}
}



