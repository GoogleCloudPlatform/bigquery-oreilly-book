

The code in this directory accompanies [this blog post](https://medium.com/@lakshmanok/graph-data-analysis-with-cypher-and-spark-sql-on-cloud-dataproc-861ba6b7b648)

To try the scala code interactively:
* Create Spark 3 Dataproc Cluster
* Switch to VM instances tab and click on SSH button
* In SSH window, type:
  ```spark-shell --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar --packages=org.opencypher:morpheus-spark-cypher:0.4.2```
* Copy-paste lines starting val BUCKET = ... into the REPL

Or you can simply submit the code (find_routes.scala) and it will install the jars and packages
