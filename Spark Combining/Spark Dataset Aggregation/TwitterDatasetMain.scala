package tc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object TwitterDatasetMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }


	val spark = SparkSession.builder().appName("AppName").config("spark.master", "local").getOrCreate()

  	val ds = spark.read.csv(args(0)).groupBy("_c1").count()
      //group the dataset by key and count the number of rows it has
	print("*********************************************\n*********************\n***********")
	print ("" + ds.explain())
	print("*********************************************\n*********************\n***********")       

	ds.write
    .format("com.databricks.spark.csv")
    .option("header", "false")
    .save(args(1))
  }
}