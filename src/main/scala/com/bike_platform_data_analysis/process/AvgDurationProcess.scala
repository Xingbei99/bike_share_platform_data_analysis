package com.bike_platform_data_analysis.process

import com.bike_platform_data_analysis.conf.BikePlatformConf
import com.bike_platform_data_analysis.io.BikeTripReader
import com.bike_platform_data_analysis.util.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

// Calculate average duration of trips of each user on the processed date
//Output: Write bike share data on the processed data to output path.
object AvgDurationProcess extends Logging with BikeTripReader {
  //Store selected fields in a list to increase readability
  val fields = List("user_id",
    "subscriber_type",
    "start_station_id",
    "end_station_id",
    "zip_code")
  val avgDurationSec = "avg_duration_sec"

  def main(args: Array[String]): Unit = {
    val conf = new BikePlatformConf(args)
    val spark = SparkSession.builder()
                            .appName("bike_share_avg_duration")
                            .getOrCreate()
    val outPath = Utils.pathGenerator(conf.outputDataPath(), conf.datePrefix(), conf.processDate())
    avgDurationCalculator(spark, conf, outPath)
  }

  def avgDurationCalculator(spark: SparkSession, conf: BikePlatformConf, outputPath: String): Unit = {
    val bikeShareTripDf = readBikeShareTrip(conf, spark)

    //use groupBy to group rows by unique users,
    //use agg() and avg() to calculate average duration of trips for each user today.
    val bikeShareTripAggDf = bikeShareTripDf
      .groupBy(fields.map(col):_*)
      .agg(avg(col("duration_sec")).as(avgDurationSec))

    bikeShareTripAggDf.coalesce(1).write.mode(SaveMode.Overwrite).json(outputPath)
  }
}
