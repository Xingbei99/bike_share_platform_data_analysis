package com.bike_platform_data_analysis.util

import com.bike_platform_data_analysis.conf.BikePlatformConf
import org.apache.spark.sql.DataFrame
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

//Defines some utility functions used for data processing.
object Utils extends Logging {

  //Select columns of input data frame based on resources/column_selection.conf and sourcekey
  def selectColumns(conf: BikePlatformConf, sourceKey: String, inputDf: DataFrame): DataFrame = {
    val fields = getListFromConf(conf.columnSelectionConfFile(), sourceKey).map(col)
    val outputDf = inputDf.select(fields: _*)
    outputDf
  }

  def getListFromConf(configFileName: String, confKey: String): List[String] = {
    try {
      ConfigFactory.load(configFileName).getStringList(confKey).toList
    } catch {
      case e: Exception =>
        logError(s"*** Error parsing for $confKey as List[String] from $configFileName ***\n${e.getMessage}")
        List[String]()
    }
  }

  def pathGenerator(inputParentPath: String, datePrefix: String, processDate: String): String = {
    s"$inputParentPath\\$datePrefix=$processDate\\" //slash在不同系统的问题，path generator and default configuration
  }

  def dayAgoDateString(conf: BikePlatformConf, dayAgo: Int): String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val processDate: DateTime         = DateTime.parse(conf.processDate(), dateFormat)
    dateFormat.print(processDate.minusDays(dayAgo))
  }
}
