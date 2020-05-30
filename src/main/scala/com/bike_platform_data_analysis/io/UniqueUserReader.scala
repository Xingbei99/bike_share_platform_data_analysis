package com.bike_platform_data_analysis.io

import com.bike_platform_data_analysis.conf.BikePlatformConf
import com.bike_platform_data_analysis.util.Utils
import com.bike_platform_data_analysis.process.UserProcess.spark
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

// Read the list of all users who joined by the specified date
trait UniqueUserReader extends Logging{
  def readUserInfo(conf: BikePlatformConf, spark: SparkSession, date: String): DataFrame = {
    //Generate input path to read from
    val inputPath = Utils.pathGenerator(conf.uniqueUsersPath(), conf.datePrefix(), date)

    logInfo("reading from %s".format(inputPath))

    //Spark will throw exception when there's no data to read. When there are no
    //new users on a given date, we catch the exception and return an empty frame
    //with the same schema.
    //
    //emptyDataFrame is an API provided by spark to create an empty dataframe with
    //empty schema. Can be used to create empty dataframes with specified schema
    //if used with withColumn().
    //
    //When helping create an empty dataframe, withColumn() should be passed the column
    //name and a null default value for data of that column.
    val inputUniqueUsersDf: DataFrame = try { //reading unique user list
      Some(spark.read.json(inputPath)).get
    } catch {
      case e: Exception => spark.emptyDataFrame.withColumn("user_id", lit(null: StringType))
        .withColumn("first_timestamp", lit(null: StringType))
    }

    inputUniqueUsersDf
  }
}
