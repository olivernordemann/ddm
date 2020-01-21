package de.hpi.spark_tutorial

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{collect_set, explode}

import scala.collection.GenIterable

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    import spark.implicits._

    def mapFileToValuesAndColumnNames(file: String): DataFrame = {
      val data = spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .csv(file)
        .as[String]

      val sepColumns = data.columns.flatMap(row => row.split(";"))
      data.flatMap(row => row.split(";").zip(sepColumns)).toDF("value", "columnName")
    }

    val completeData = inputs.map(file => mapFileToValuesAndColumnNames(file)).reduce(_.union(_))

    val aggregatedValues = completeData.groupBy("value").agg(collect_set("columnName"))
    aggregatedValues.printSchema()
    aggregatedValues.show()

    val columnSets = aggregatedValues.select("collect_set(columnName)")
    columnSets.printSchema()
    columnSets.show()

    val inclusionSetsWithSelf = columnSets.withColumn("columnName", explode($"collect_set(columnName)"))
    inclusionSetsWithSelf.printSchema()
    inclusionSetsWithSelf.show()

    def removeOwnColumnNameFromInclusionSet(columnName: String, inclusionSet: Seq[String]): Seq[String] = {
      inclusionSet.filterNot(element => element == columnName)
    }

    val inclusionSets = inclusionSetsWithSelf.map(row => removeOwnColumnNameFromInclusionSet(row.getAs[String]("columnName"), row.getAs[Seq[String]]("collect_set(columnName)")))
    inclusionSets.printSchema()
    inclusionSets.show()

    
  }
}
