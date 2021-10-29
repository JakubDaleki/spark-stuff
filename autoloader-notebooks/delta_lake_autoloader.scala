// Databricks notebook source
// MAGIC %md
// MAGIC ***Configuration***

// COMMAND ----------

val storageAccount = "accountName"
val bronzeLocation = s"abfss://bronze@${storageAccount}.dfs.core.windows.net/"
val silverLocation = s"abfss://silver@${storageAccount}.dfs.core.windows.net/"

val tables = List("Table1", "Table2")

// COMMAND ----------

import java.time._
import java.time.format.DateTimeFormatter

def getCurrentDatetime(): String = {
  val zone = ZoneId.of("Europe/London")
  val time = LocalDateTime.now(zone)
  time.toString()
}

// COMMAND ----------

import io.delta.tables._
import org.apache.spark.sql.functions._

def deduplicateDf(df: DataFrame): DataFrame = {
  val sortedDf = if (df.columns.contains("LastModifiedDate"))
                     df.withColumn("LastModifiedDate", to_timestamp(col("LastModifiedDate"))).sort(desc("LastModifiedDate"))
                 else df
  
  sortedDf.dropDuplicates(Seq("Id"))
}

def upsertDeltaTable(df: DataFrame, dataDir: String): Unit = {
  val deltatable_df = DeltaTable.forPath(spark, dataDir)
  deltatable_df.alias("t")
    .merge(df.alias("s"), "s.Id = t.Id")
    .whenMatched().updateAll()
    .whenNotMatched().insertAll()
    .execute()
}

def upsertEachBatch(df: DataFrame, batchId: Long, tableName: String, dataDir: String): Unit = {
  val dfDeduplicated = deduplicateDf(df)
  dfDeduplicated.persist()
  val isInitalized = DeltaTable.isDeltaTable(spark, dataDir)
  if (isInitalized) {
    upsertDeltaTable(dfDeduplicated, dataDir)
  } else {
    dfDeduplicated.write.format("delta").save(dataDir)
  }
  writeDfToSql(dfDeduplicated, tableName)
  
  dfDeduplicated.unpersist()
}

// COMMAND ----------

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger

def readAutoloaderDf(sourceLocation: String, schemaLocation: String): DataFrame = {
  val currentDatetime = getCurrentDatetime()
  val df = spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", schemaLocation)
    .load(sourceLocation)
    .withColumn("IngestionDatetime", lit(currentDatetime))
    .withColumn("IngestionDatetime", to_timestamp(col("IngestionDatetime")))
  
  df
}

def writeAutoloaderDf(df: DataFrame, silverDeltaLocation: String, checkpointLocation: String, tableName: String): StreamingQuery = {
   // check if delta location exists - if it doesn't it is the first run
  val isInitalized = DeltaTable.isDeltaTable(spark, silverDeltaLocation)
  
  // declare common DataStreamWriter 
  val query = df.writeStream
      .option("mergeSchema", "true")
      .option("checkpointLocation", checkpointLocation)
      .queryName(tableName)
      .foreachBatch{(batchDF: DataFrame, batchId: Long) => 
         upsertEachBatch(batchDF, batchId, tableName, silverDeltaLocation)}
      .trigger(Trigger.Once()).start(silverDeltaLocation)
 
  query
}

// COMMAND ----------

def loadNewData(tableName: String): StreamingQuery = {
  val destDir = silverLocation + tableName
  val sourceLocation = bronzeLocation + tableName
  val schemaLocation = destDir + "/schema/"
  val silverDeltaLocation = destDir + "/data"
  val checkpointLocation = destDir + "/checkpoint"
  
  val df = readAutoloaderDf(sourceLocation, schemaLocation)
  writeAutoloaderDf(df, silverDeltaLocation, checkpointLocation, tableName)
}
  

// COMMAND ----------

def insertionSteps(table: String): String = {
  val stream = loadNewData(table)
  // if UnknownFieldException was thrown, this command will fail with StreamingQueryException
  stream.awaitTermination()
  stream.lastProgress.toString()
}

// COMMAND ----------

def persistLog(logs: String): Unit = {
  val fileName = s"log_${getCurrentDatetime}.json"
  val pathAffix = s"logs/${fileName}"
  dbutils.fs.put(silverLocation + pathAffix, logs)
}

// COMMAND ----------

import org.apache.spark.sql.streaming._

var logs = List[String]()
// perform upserting for each table
for (table <- tables) {
  try {
    val progress = insertionSteps(table)
    logs = logs :+ progress
  } catch {
      case e: RuntimeException => {
        val message = s"""{\n\t"tableName": "${table}",\n\t"status": "Directory does not exisit, please create missing directory or upload missing files"\n}"""
        logs = logs :+ message
      }
      case e: StreamingQueryException => {
        val message = s"""{\n\t"tableName": "${table}",\n\t"status": "New column added for a table, reinserting later"\n}"""
        logs = logs :+ message
      }
  }
}

persistLog(logs.mkString("[", ",\n", "]"))
