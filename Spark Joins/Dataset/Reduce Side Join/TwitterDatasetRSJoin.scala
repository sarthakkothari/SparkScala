package tc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import spark.implicits._

import org.apache.log4j.LogManager
import org.apache.log4j.Level

import java.io._

object TwitterDatasetRSJoin {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    
    
    val conf = new SparkConf().setAppName("AppName")
    val sc = new SparkContext(conf)
		val spark = SparkSession.builder().appName("AppName").config("spark.master", "local").getOrCreate() //Create spark object
	

    

		val ds = spark.read.csv(args(0)).toDF("From", "To") //read file
		val ds_filter = ds.filter("From <=65000 and To <=65000") // filter file
		val ds_new = ds_filter.as("FromDS")
					.join(ds_filter.as("ToDS"), $"FromDS.To" === $"ToDS.From") // self join
					.select($"FromDS.From".as("From"), $"ToDS.To".as("To")) // select only 2 elements
					.as("PathDS")
					.join(ds_filter.as("DS"), $"PathDS.To" === $"DS.From" &&  $"PathDS.From" === $"DS.To" ) // join to Filtered DS with To = From and From = to
	
	
	
		print("*********************************************\n*********************\n***********")
		val res = ds_new.count() // get count
		print ("" + res)     // print count
		print("*********************************************\n*********************\n***********")  
		
		val ans = sc.parallelize(Seq(res))
		ans.saveAsTextFile(args(1))

  }
}