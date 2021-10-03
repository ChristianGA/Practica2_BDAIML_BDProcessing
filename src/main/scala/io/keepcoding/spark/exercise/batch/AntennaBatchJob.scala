package io.keepcoding.spark.exercise.batch

import java.time.OffsetDateTime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object AntennaBatchJob extends BatchJob {

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark SQL KeepCoding Base")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(s"${storagePath}/data")
      .filter(
        $"year" === filterDate.getYear &&
          $"month" === filterDate.getMonthValue &&
          $"day" === filterDate.getDayOfMonth &&
          $"hour" === filterDate.getHour
      )
  }

  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    antennaDF.as("antenna")
      .join(
        metadataDF.as("metadata"),
        $"antenna.id" === $"metadata.id"
      ).drop($"metadata.id")
  }

  override def computeBytesCountByAntenna(dataFrame: DataFrame: DataFrame = {
    dataFrame
      .select($"bytes", $"timestamp", $"antenna_id")
      .groupBy($"antenna_id")
      .agg(sum($"bytes").as("total_bytes"))
      .withColumn("type", lit("total_bytes"))
  }

  override def computeBytesCountByUserEmail(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"bytes", $"timestamp", $"email")
      .groupBy( $"email")
      .agg(sum($"bytes").as("user_total_bytes"))
      .withColumn("type", lit("user_total_bytes"))
  }

  override def computeBytesCountByApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"bytes", $"timestamp", $"app")
      .groupBy($"app")
      .agg(sum($"bytes").as("app_total_bytes"))
      .withColumn("type", lit("app_total_bytes"))
  }

  override def computeOverQuoteUsersPerHour(dataFrame: DataFrame): DataFrame = {
    val dataframeCache = dataFrame.cache()
    val totalBytes = dataframeCache
      .select($"timestamp", $"email", $"bytes", $"quota")
      .groupBy($"email", window($"timestamp", "1 hour").as("timestamp"))
      .agg(sum($"bytes").as("user_total_bytes"))
      .select($"email", col("timestamp.start").as("timestamp"), col("user_total_bytes"))

    val quoteEmails = dataframeCache
      .select($"timestamp", $"email", $"quota")
      .groupBy($"email", window($"timestamp", "1 hour").as("timestamp"))
      .agg(avg($"quota").as("quote"))
      .select($"email", col("timestamp.start").as("timestamp"), col("quote"))

    val joinDF = totalBytes
      .join(quoteEmails, totalBytes("email").as("email") === quoteEmails("email").as("email"))
      .drop(quoteEmails("timestamp"))
      .drop(quoteEmails("email"))

    joinDF
      .select($"timestamp", $"email", $"bytes", $"quota")
      .where(col("user_total_bytes") > col("quote"))
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = {
    dataFrame
      .write
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(s"${storageRootPath}/historical")
  }

  def main(args: Array[String]): Unit = run(args)
}
