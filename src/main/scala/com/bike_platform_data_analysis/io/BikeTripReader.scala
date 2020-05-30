package com.bike_platform_data_analysis.io

import com.bike_platform_data_analysis.conf.BikePlatformConf
import com.bike_platform_data_analysis.util.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

trait BikeTripReader extends Logging{
  //Read today's bike share data and output its dataframe with selected columns
  def readBikeShareTrip(conf: BikePlatformConf, spark: SparkSession): DataFrame = {
    val inputPath = Utils.pathGenerator(conf.inputBikeDataPath(), conf.datePrefix(), conf.processDate())

    logInfo("Reading today's bike share data from %s".format(inputPath))

    //Generate an empty dataframe with the same schema if today's bike share data is empty
    val bikeShareDf: DataFrame = try {
      Some(spark.read.json(inputPath)).get
    } catch {
      case e: Exception => spark.emptyDataFrame
        .withColumn("user_id", lit(null: StringType))
        .withColumn("subscriber_type", lit(null: StringType))
        .withColumn("start_station_id", lit(null: StringType))
        .withColumn("end_station_id", lit(null: StringType))
        .withColumn("zip_code", lit(null: StringType))
        .withColumn("duration_sec", lit(null: DoubleType))
        .withColumn("start_timestamp", lit(null: StringType))
    }
    Utils.selectColumns(conf, "bike.share.trip", bikeShareDf)
  }

  // Read bike share data given number of days ago (number of days ago specified by BikePlatformConf)
  def readDayAgoBikeShareData(conf: BikePlatformConf, spark: SparkSession): DataFrame = {
    val path = dayAgoDataReadPath(conf)
    logInfo("reading from %s".format(path))

    val bikeShareDf: DataFrame = try {
      Some(spark.read.json(path)).get
    } catch {
      case e: Exception => spark.emptyDataFrame
        .withColumn("user_id", lit(null: StringType))
        .withColumn("subscriber_type", lit(null: StringType))
        .withColumn("start_station_id", lit(null: StringType))
        .withColumn("end_station_id", lit(null: StringType))
        .withColumn("zip_code", lit(null: StringType))
        .withColumn("avg_duration_sec", lit(null: DoubleType))
    }
    bikeShareDf
  }

  def dayAgoDataReadPath(conf: BikePlatformConf): String = {
    val dateString = Utils.dayAgoDateString(conf, conf.dayAgo())

    val path : String = conf.dayAgo() match {
      case 1 => Utils.pathGenerator(conf.outputDataPath(), conf.datePrefix(), dateString)
      case 3 => Utils.pathGenerator(conf.outputDataPath()+"/1", conf.datePrefix(), dateString)
      case 7 => Utils.pathGenerator(conf.outputDataPath()+"/3", conf.datePrefix(), dateString)
      case _ => throw new Exception("input date is invalid")
    }
    path
  }

  def dayAgoDataWritePath(conf: BikePlatformConf): String = {
    val dateString = Utils.dayAgoDateString(conf, conf.dayAgo())

    val path : String = conf.dayAgo() match {
      case 1 => Utils.pathGenerator(conf.outputDataPath()+"/1", conf.datePrefix(), dateString)
      case 3 => Utils.pathGenerator(conf.outputDataPath()+"/3", conf.datePrefix(), dateString)
      case 7 => Utils.pathGenerator(conf.outputDataPath()+"/7", conf.datePrefix(), dateString)
      case _ => throw new Exception("input date is invalid")
    }
    path
  }
}
