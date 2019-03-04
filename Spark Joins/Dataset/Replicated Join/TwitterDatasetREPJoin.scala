package tc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.broadcast

import org.apache.log4j.LogManager
import org.apache.log4j.Level

object TwitterDatasetREPJoin {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }

		val conf = new SparkConf().setAppName("AppName")
			val sc = new SparkContext(conf)
		val spark = SparkSession.builder().appName("AppName").config("spark.master", "local").getOrCreate() // create spark object
		import spark.implicits._

			

		val ds = spark.read.csv(args(0)).toDF("From", "To") // read file
		val ds_filter = ds.filter("From <=125000 and To <=125000") // filter file
		val ds_new = ds_filter.as("FromDS")
					.join(broadcast(ds_filter.as("ToDS")), $"FromDS.To" === $"ToDS.From") // self join and broadcast to all mappers
					.select($"FromDS.From".as("From"), $"ToDS.To".as("To")) // remove middle edge of path len 2
					.as("PathDS")
					.join(broadcast(ds_filter.as("DS")), $"PathDS.To" === $"DS.From" &&  $"PathDS.From" === $"DS.To" ) // join to filtered ds with 2 condition To = From and From = To, boradcast it too all mappers
		
		val res = ds_new.count() // calc count
		print ("" + res)     // print count
		
		val ans = sc.parallelize(Seq(res))
		ans.saveAsTextFile(args(1))
	
  }
}