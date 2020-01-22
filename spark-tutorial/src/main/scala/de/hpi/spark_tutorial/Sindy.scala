package de.hpi.spark_tutorial

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{collect_set, explode}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    import spark.implicits._

    val completeData = inputs.map(file => {
        val data = spark.read
          .option("inferSchema", "false")
          .option("header", "true")
          .option("sep", ";")
          .csv(file)

        val sepColumns = data.columns
        data.flatMap(_.toSeq.asInstanceOf[Seq[String]].zip(sepColumns)).toDF("value", "columnName")
      })
      .reduce(_.union(_))
      .distinct()

    val inclusionSets = completeData
      .groupBy("value")
      .agg(collect_set("columnName"))
      .select("collect_set(columnName)")
      .distinct()
      .withColumn("columnName", explode($"collect_set(columnName)"))

    def removeOwnColumnNameFromInclusionSet(row: Row): Seq[String] = {
      row
        .getAs[Seq[String]]("collect_set(columnName)")
        .filterNot(_ == row.getAs[String]("columnName"))
    }

    val inclusionSet = inclusionSets
      .map(row => (row.getAs[String]("columnName"), removeOwnColumnNameFromInclusionSet(row)))
      .toDF("columnName", "inclusionSet")

    val groupedInclusionSets = inclusionSet
      .groupBy("columnName")
      .agg(collect_set("inclusionSet"))

    val finalInclusionSet = groupedInclusionSets
      .map(row => (row.getAs[String]("columnName"), row.getAs[Seq[Seq[String]]]("collect_set(inclusionSet)")
      .reduce(_.intersect(_))))
      .toDF("columnName", "inclusionSet")
      .filter(_.getAs[Seq[String]]("inclusionSet").nonEmpty)
      .orderBy("columnName")

    finalInclusionSet.collect().foreach{ row => println(row.getAs[String]("columnName") + " > " + row.getAs[Seq[String]]("inclusionSet").mkString(", "))}
  }
}
