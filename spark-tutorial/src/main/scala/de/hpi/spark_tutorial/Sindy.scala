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

    val columnSets = aggregatedValues.select("collect_set(columnName)")

    val inclusionSetWithSelf = columnSets.withColumn("columnName", explode($"collect_set(columnName)"))

    def removeOwnColumnNameFromInclusionSet(row: Row): Seq[String] = {
      row.getAs[Seq[String]]("collect_set(columnName)").filterNot(element => element == row.getAs[String]("columnName"))
    }

    val inclusionSet = inclusionSetWithSelf.map(row => (row.getAs[String]("columnName"), removeOwnColumnNameFromInclusionSet(row))).toDF("columnName", "inclusionSet")

    val groupedInclusionSets = inclusionSet.groupBy("columnName").agg(collect_set("inclusionSet"))
//    groupedInclusionSets.printSchema()
//    groupedInclusionSets.show()

    val intersectedInclusionSet = groupedInclusionSets.map(row => (row.getAs[String]("columnName"), row.getAs[Seq[Seq[String]]]("collect_set(inclusionSet)").reduce(_.intersect(_)))).toDF("columnName", "inclusionSet")
    intersectedInclusionSet.printSchema()
    intersectedInclusionSet.show()

//    val explodedInclusions = intersectedInclusionSet.withColumn("includedIn", explode($"inclusionSet")).select("columnName", "includedIn")
//    explodedInclusions.printSchema()
//    explodedInclusions.show()


  }
}
