package de.hpi.spark_tutorial

import org.apache.spark.sql.{SparkSession}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SimpleSpark extends App {

  override def main(args: Array[String]): Unit = {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //------------------------------------------------------------------------------------------------------------------
    // Setting up a Spark Session
    //------------------------------------------------------------------------------------------------------------------

    // Recommended level of parallelism is 3-4 per core; observed better performance when setting highers
    val LEVEL_PARALLELISM = 8

    // Parse command line arguments and set defaults
    var numCores = 4
    var pathToDataSet = "./TPCH"

    args.sliding(2, 2).toList.collect {
      case Array("--cores", argCores: String) => numCores = argCores.toInt
      case Array("--path", argPath: String) => pathToDataSet = argPath
    }

    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master(s"local[${numCores}]")
      // .config("spark.sql.shuffle.partitions", (numCores * parallelism).toString)
      .config("spark.sql.shuffle.partitions", (numCores * LEVEL_PARALLELISM).toString)
      // .config("spark.default.parallelism", (numCores * parallelism).toString)
      .config("spark.default.parallelism", (numCores * LEVEL_PARALLELISM).toString)
    val spark = sparkBuilder.getOrCreate()

    def time[R](block: => R): R = {
      val t0 = System.currentTimeMillis()
      val result = block
      val t1 = System.currentTimeMillis()
      println(s"Execution: ${t1 - t0} ms")
      result
    }

    val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
      .map(name => s"${pathToDataSet}/tpch_$name.csv")

    time {Sindy.discoverINDs(inputs, spark)}
  }
}
