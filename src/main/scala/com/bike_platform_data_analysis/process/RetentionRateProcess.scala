package com.bike_platform_data_analysis.process

import com.bike_platform_data_analysis.conf.BikePlatformConf
import com.bike_platform_data_analysis.io._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

//This process will be run after UniqueUserProcess
object RetentionRateProcess extends Logging with UniqueUserReader with BikeTripReader {

  def retentionPrep(spark: SparkSession, conf: BikePlatformConf): Unit = {

    //Calculate how many days each today's user has joined the system:
    //1.Read today's bike share data and list of users who join by today
    //2.Join the two dataframes by merging user_id
    //3.for each user, start_timestamp - first_timestamp to calculate the result
    val bikeDf = readBikeShareTrip(conf, spark) //read today's bike share data
    val userDf = readUserInfo(conf, spark, conf.processDate()) //read list of users joined by today

    //Left join: Return all rows in left table no matter if there's a match in right table:
    //           (1) If user_id of a row in left table does not exist in right table, the row
    //               will be returned, the missing right table columns will be filled with null
    //               (first_timestamp)
    //               *In this example, right table has all user joining by today, so each user id
    //                in left table is guaranteed to exist in right table
    //           (2) If user_id of a row in right table does not exist in left table, the row
    //               will be deleted
    //
    //Join API: left.join(right: Dataframe[_], joinExprs: Column, joinType: String): DataFrame
    //        *Note: both columns of joinExprs exist after joining -> remember to drop!
    //               use === for joinExprs
    //        *Use df.col("") to refer to a column of a dataframe
    //
    //effect: today's data is appended a new column called "first_timestamp"
    val joinedBikeDf = bikeDf.join(userDf,
      bikeDf.col("user_id") === userDf.col("user_id"), "left")
      .drop(bikeDf.col("user_id"))


    val bikeUserAgeDf = joinedBikeDf
      .withColumn("user_age_days",
        datediff(to_date(col("start_timestamp")), to_date(col("first_timestamp"))))

    // Filter data based on which day ago's retention rate we want to calculate
    val bikeFilteredDf : DataFrame = conf.dayAgo() match {
      case 1 => bikeUserAgeDf.filter(col("user_age_days") === 1)
      case 3 => bikeUserAgeDf.filter(col("user_age_days") === 3)
      case 7 => bikeUserAgeDf.filter(col("user_age_days") === 7)
      case _ => throw new Exception("input date is invalid")
    }

    // deduplicate users with multiple trips today
    val bikeFilteredDf2 = bikeFilteredDf.select("user_id", "user_age_days").distinct()

    // Read bike share data given days ago, whose retention rate we want to calculate. Join its data frame with bikeFilteredDf2
    // The result dataframe is bike share data given days ago with an additional "user_age_days" column. Rows with a non-null value
    // for "user_age_days" column are users who joined given days ago and returned today.
    val dayAgoBikeDf = readDayAgoBikeShareData(conf, spark)
    val dayAgoRetPrepDf = dayAgoBikeDf
      .join(bikeFilteredDf2, dayAgoBikeDf.col("user_id") === bikeFilteredDf2.col("user_id"), "left")
      .drop(bikeFilteredDf2.col("user_id"))

    // If there are no users joining given days ago and returning today, we still add user_age_days column to unify output's schemas
    if(!dayAgoRetPrepDf.columns.contains("user_age_days") || dayAgoRetPrepDf.count() == 0){
      logInfo("didn't find anyone fit into %s day ago".format(conf.dayAgo()))
      val dayAgoRetDfWithAge = dayAgoRetPrepDf.withColumn("user_age_days", lit(0))
      saveRetentionData(dayAgoRetDfWithAge, conf)
    } else {
      saveRetentionData(dayAgoRetPrepDf, conf)
    }
  }

  //Saves the retention dataframe given days ago used for calculating retention rates.
  def saveRetentionData(df: DataFrame, conf: BikePlatformConf): Unit = {
    val groupbyFields = AvgDurationProcess.fields :+ AvgDurationProcess.avgDurationSec :+ "user_age_days"
    // uses groupBy to deduplicate users
    val dayAgoRetDf = df.groupBy(groupbyFields.map(col):_*)
      .agg(max(when(df.col("user_age_days") === 1, 1).otherwise(0)).alias("retention_1"),
        max(when(df.col("user_age_days") === 3, 1).otherwise(0)).alias("retention_3"),
        max(when(df.col("user_age_days") === 7, 1).otherwise(0)).alias("retention_7"))

    val outputPath = dayAgoDataWritePath(conf)

    dayAgoRetDf.coalesce(1).write.mode(SaveMode.Overwrite).json(outputPath)
  }
}
