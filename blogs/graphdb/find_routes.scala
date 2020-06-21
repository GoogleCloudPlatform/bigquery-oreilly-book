/*
Copyright 2019 Google LLC.
SPDX-License-Identifier: Apache-2.0

This code accompanies this blog post:
https://medium.com/@lakshmanok/graph-data-analysis-with-cypher-and-spark-sql-on-cloud-dataproc-861ba6b7b648
*/


import org.apache.spark.sql.SparkSession

// To run in a shell, replace this section by ssh-into Dataproc instance
// and typing in
// spark-shell --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar --packages=org.opencypher:morpheus-spark-cypher:0.4.2

val BQ_JAR = "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"  // CHANGE as needed
val CYPHER = "org.opencypher:morpheus-spark-cypher:0.4.2"
val spark = (SparkSession.builder().appName("sfbus")
  .config('spark.jars', BQ_JAR)
  .config('spark.jars.packages', CYPHER)
  .getOrCreate())

// Start here if trying it out interactively
val BUCKET = "ai-analytics-solutions-kfpdemo"     // CHANGE as needed
spark.conf.set("temporaryGcsBucket", BUCKET)
spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", true) // https://github.com/opencypher/morpheus/issues/948

val stopTimesDF = (spark.read.format("bigquery")
  .option("table", "bigquery-public-data:san_francisco_transit_muni.stop_times")
  .option("filter", "arrives_next_day = false AND dropoff_type = 'regular'")
  .load().cache())
stopTimesDF.createOrReplaceTempView("stopTimes")
stopTimesDF.printSchema()

// trips contain stops
val stopsDF = spark.sql("SELECT DISTINCT stop_id AS id, stop_id AS stop_number FROM stopTimes")
val tripsDF = spark.sql("SELECT DISTINCT trip_id AS id, trip_id AS trip_number FROM stopTimes")
val containsDF = spark.sql("SELECT DISTINCT trip_id AS source, stop_id AS target, stop_sequence, " +
   "CONCAT(trip_id, stop_id) AS id FROM stopTimes")

// build graph
import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.{MorpheusNodeTable, MorpheusRelationshipTable}
import spark.sqlContext.implicits._

val stopTable = MorpheusNodeTable(Set("Stop"), stopsDF)
val tripTable = MorpheusNodeTable(Set("Trip"), tripsDF)
val containsTable = MorpheusRelationshipTable("CONTAINS", containsDF)
implicit val morpheus: MorpheusSession = MorpheusSession.local()
val graph = morpheus.readFrom(stopTable, tripTable, containsTable)

// Query: which trips include stop number 15104?
val result = graph.cypher("""
  |MATCH
  | (s1:Stop {stop_number: 15104})<-[c1:CONTAINS]-(t1:Trip)
  |RETURN t1.trip_number AS trip, c1.stop_sequence AS seq
""".stripMargin)
result.records.table.df.toDF("trip", "seq").createOrReplaceTempView("results")
val resultsTable = spark.sql("SELECT * FROM results")
resultsTable.show()



// Query: Find the route that trip 8962069 takes
val result = graph.cypher("""
  |MATCH
  | (s1:Stop)<-[c1:CONTAINS]-(t1:Trip)
  |WHERE t1.trip_number = 8962069
  |RETURN s1.stop_number AS stop, c1.stop_sequence AS seq
  |ORDER BY c1.stop_sequence ASC
""".stripMargin)
result.records.table.df.toDF("stop", "seq").createOrReplaceTempView("results")
val resultsTable = spark.sql("SELECT * FROM results")
resultsTable.show()

// Query: Find the "next" stop for each stop
val result = graph.cypher("""
  |MATCH
  | (s1:Stop)<-[c1:CONTAINS]-(t1:Trip)
  |MATCH
  | (s2:Stop)<-[c2:CONTAINS]-(t1)
  |WHERE t1.trip_number = 8962069 AND
  |      c2.stop_sequence = c1.stop_sequence+1
  |RETURN s1.stop_number AS stop, s2.stop_number AS next, c1.stop_sequence AS seq
  |ORDER BY c1.stop_sequence ASC
""".stripMargin)
result.records.table.df.toDF("stop", "next", "seq").createOrReplaceTempView("results")
val resultsTable = spark.sql("SELECT * FROM results")
resultsTable.show()

// Create new edges in the graph to correspond to "next" stop
morpheus.catalog.store("stopsGraph", graph)
val routeGraph = graph.cypher("""
  |MATCH
  | (s1:Stop)<-[c1:CONTAINS]-(t1:Trip)
  |MATCH
  | (s2:Stop)<-[c2:CONTAINS]-(t1)
  |WHERE c2.stop_sequence = c1.stop_sequence+1
  |CONSTRUCT on stopsGraph
  |  CREATE (s1)-[:NEXT {trip_number: t1.trip_number, stop_seq: c2.stop_sequence}]-> (s2)
  |RETURN GRAPH
""".stripMargin).graph


val result = routeGraph.cypher("""
  |MATCH
  | (s1:Stop)<-[n1:NEXT]-(s2:Stop)
  |MATCH
  | (s2)-[n2:NEXT]->(s3:Stop)
  |WHERE n1.trip_number = 8962069 AND n2.trip_number = n1.trip_number
  |RETURN s1.stop_number AS prev, s2.stop_number AS curr, s3.stop_number AS next, n1.stop_seq AS seq
  |ORDER BY n1.stop_seq ASC
""".stripMargin)
result.records.table.df.toDF("prev", "curr", "next", "seq").createOrReplaceTempView("results")
val resultsTable = spark.sql("SELECT * FROM results")
resultsTable.show()

