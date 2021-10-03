package io.keepcoding.spark.exercise.streaming

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(timestamp: Timestamp, id: String, metric: String, value: Long)

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame

  def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame


  def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  def computeBytesByAntenna(dataFrame: DataFrame): DataFrame

  def computeBytesByUserId(dataFrame: DataFrame): DataFrame

  def computeBytesByApp(dataFrame: DataFrame): DataFrame


  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]

  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic, jdbcUri, jdbcMetadataTable, aggJdbcTable, jdbcUser, jdbcPassword, storagePath) = args
    println(s"Running with: ${args.toSeq}")

    val kafkaDF = readFromKafka(kafkaServer, topic)
    val antennaDF = parserJsonData(kafkaDF)
    val metadataDF = readAntennaMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val antennaMetadataDF = enrichAntennaWithMetadata(antennaDF, metadataDF)
    val storageFuture = writeToStorage(antennaDF, storagePath)

    val BytesByAntennasDF = computeBytesByAntenna(antennaMetadataDF)
    val BytesByIdDF = computeBytesByUserId(antennaMetadataDF)
    val BytesByAppDF = computeBytesByApp(antennaMetadataDF)


    val aggFutureAntenna = writeToJdbc(BytesByAntennasDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val aggFutureUser = writeToJdbc(BytesByIdDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val aggFutureApp = writeToJdbc(BytesByAppDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)

    Await.result(Future.sequence(Seq(aggFutureAntenna, aggFutureUser, aggFutureApp, storageFuture)), Duration.Inf)

    spark.close()
  }

}
