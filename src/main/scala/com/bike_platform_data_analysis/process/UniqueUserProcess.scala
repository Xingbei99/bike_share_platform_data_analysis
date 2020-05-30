package com.bike_platform_data_analysis.process

import com.bike_platform_data_analysis.conf.BikePlatformConf
import com.bike_platform_data_analysis.io._
import com.bike_platform_data_analysis.util.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

//Process to get a list of all users who join by today
//Input: list of all users who join by yesterday
//Output: list of all users who join by today
//Notes on defining processes:
//1.Uses processes to define different jobs, so must have a driver
object UniqueUserProcess extends Logging with UniqueUserReader with BikeTripReader{
  val spark = SparkSession
    .builder() //builder is the ctor for SparkSession
    .appName("Unique-users")
    .getOrCreate() //Spark object follows factory pattern, should have only one across the project

  //Get list of all users who join by today
  def uniqueUser(uniqueUsersPath: String, inputBikeSharePath: String, conf: BikePlatformConf): Unit = {
    //dayAgoNewUsersDf stores all users who joined before today.
    val dayAgoNewUsersDf = readUserInfo(conf, spark, Utils.dayAgoDateString(conf, 1))
    val todayBikeDf = readBikeShareTrip(conf, spark) //Read today's bike share data

    //Get list of today's users (including return users and the same user having multiple trips today)
    //1.Read today's bike share data and get its data frame
    //2.Transform the dataframe to have the same schema as dayAgoNewUsersDf to ease aggregation
    //?
    val todayUserDf = Utils.selectColumns(conf, "bike.unique.user", todayBikeDf)
      .withColumnRenamed("start_timestamp", "first_timestamp")


    //Get list of all users who join by today
    //1.Use unionByName() to aggregate list of today's users with list of users who joined
    //  by yesterday
    //2.Merge list of today's users with list of users who joined by yesterday using groupBy
    //  Ex: ["123", "4-14"] in yesterday's list and two ["123, "4-15"]s in today's list
    //      After grouped by "user_id", they will become one row with user id of "123"
    //      Use agg to calculate first_timestamp value of the new row, which should be min
    //      first_timestamp value across all merged rows: "4-14"
    //
    //Notes on using groupBy:
    //1.Before groupBy, must make sure the data frames to aggregate have the same schema
    //  and union them with unionByName to get aligned aggregated dataframe
    //2.After groupBy()s, must use agg()s to calculate new values for rest of the columns
    //  (Not grouped by) based on their previous values before merging to prevent data loss
    val uniqueUserDf = dayAgoNewUsersDf.unionByName(todayUserDf)
      .groupBy("user_id")
      .agg(min("first_timestamp").as("first_timestamp"))

    uniqueUserDf.coalesce(1).write.mode(SaveMode.Overwrite).json(uniqueUsersPath)
  }

  //Driver for uniqueUser()
  def main(args: Array[String]): Unit = {
    val conf = new BikePlatformConf(args) //Create configuration object based on args to main()
    val inputPath = Utils.pathGenerator(conf.inputBikeDataPath(), conf.datePrefix(), conf.processDate())
    val outputPath = Utils.pathGenerator(conf.uniqueUsersPath(), conf.datePrefix(), conf.processDate())
    uniqueUser(outputPath, inputPath, conf)
  }
}
