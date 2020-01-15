package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    import spark.implicits._

    for(file <- inputs){
      val data = spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .csv(file)
        .as[String]

      data.printSchema()
      data.show(10)

      val columns = data.columns

      val sepColumns = columns.flatMap(row => row.split(";"))
      val y = data.flatMap(row => row.split(";").zip(sepColumns))
      y.show()
    }
  }
}
