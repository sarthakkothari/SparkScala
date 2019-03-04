package pr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.log4j.LogManager
import org.apache.log4j.Level

import java.io._

object TwitterDatasetPR {
  
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
    
		val conf = new SparkConf().setAppName("PR Dataset 3 Test 10Iteration")
    	val sc = new SparkContext(conf)
		val spark = SparkSession.builder().appName("PR Dataset 3 Test 10Iteration")
																			.config("spark.master", "local").getOrCreate() //Create spark object
		import spark.implicits._

		spark.sparkContext.setCheckpointDir("/tmp")
		for(i <- 1 to k2) {
			if ((i % k) != 0) {
				l =  List.concat(l, List((i, i+1)))
			}
			else
				l = List.concat(l, List((i,0)))
			l1 = List.concat(l1, List((i,1.0/k2)))
		}

		var GraphDF = l.toDF("From", "To").repartition($"From")  //read file
		var prTable = l1.toDF("ID", "PR").repartition($"ID")
		GraphDF.persist()
		prTable = prTable.union(Seq((0, 0.0)).toDF())

		var dangling = 0.0

		GraphDF = GraphDF.checkpoint(false )

	for( j <- 1 to 10) {

		var startNode = prTable.filter(($"ID" % k) === 1)
														.map(row => (row.getInt(0), 0))
														.toDF("ID", "PR")

		prTable = GraphDF.as("G")
											.join(prTable.as("P"), $"G.from" === $"P.ID")
  										.select($"To", $"PR")
											.groupBy($"To")
  										.sum()
											.select($"To".as("ID"), ($"sum(PR)").as("PR"))
  										.union(startNode)


		prTable = prTable.checkpoint(false)

		dangling = (prTable.filter($"ID" === 0).collect()(0)(1).toString.toDouble) / k2


		prTable = prTable.map(row => 	if(row.getInt(0) != 0)  (row.getInt(0), row.getDouble(1)+dangling)
																else (row.getInt(0), row.getDouble(1)))
									.toDF("ID", "PR")
		
		val prMass = prTable.where("ID != 0").select($"PR").groupBy().sum().toDF("SumMassOf"+j)


		print("\n\n\n\n\n****************************************************************")
		print(prMass.show(false))
		print("\n\n\n\n\n****************************************************************")

	}

		val ans = prTable.sort(asc("ID")).head(101)

		prTable = sc.parallelize(ans).map(row => (row.getInt(0), row.getDouble(1))).toDF("ID", "PR")

		prTable.show()

		prTable.write.csv(args(1))

  }
}



