package com.bike_platform_data_analysis.conf

import org.rogach.scallop.{ScallopConf, ScallopOption}

class BikePlatformConf(args: Seq[String]) extends ScallopConf(args) with Serializable {

  val columnSelectionConfFile: ScallopOption[String] =
    opt[String]("column.selection.config",
      descr = "Names of selected columns",
      required = false,
      default = Option("column_selection"))

  val bikeTripKey: ScallopOption[String] =
    opt[String]("bike.trip.key",
      descr = "bike trip path key",
      required = false,
      default = Option("bike-trips"))

  val env: ScallopOption[String] =
    opt[String]("env",
      descr = "names of environments the job is running in: test / stage / prod",
      required = false,
      default = Option("stage"))

  val inputBikeDataPath: ScallopOption[String] =
    opt[String]("input.bikedata.path",
      descr = "input data path of bike share data",
      required = false,
      default = env() match {
        case "test" => Option("C:\\Users\\bike-data\\bike-trips")
        case "stage" => Option("C:\\Users\\bike-data\\bike-trips")
        case "prod" => Option("C:\\Users\\bike-data\\bike-trips")
        case _ => None
          throw new Exception(s"env name error: env name can only be test, stage or prod \ncannot be ${env()}")
      })

  val inputMetaDataPath: ScallopOption[String] =
    opt[String]("input.meta.path",
      descr = "input meta data parent path",
      required = false,
      default = env() match {
        case "test" => Option("C:\\Users\\bike")
        case "stage" => Option("C:\\Users\\bike")
        case "prod" => Option("C:\\Users\\bike")
        case _ => None
          throw new Exception(s"env name error: env name can only be test, stage or prod \ncannot be ${env()}")
      })

  val outputDataPath: ScallopOption[String] =
    opt[String]("output.data.path",
      descr = "output data parent path",
      required = false,
      default = env() match {
        case "test" => Option("C:\\Users\\output")
        case "stage" => Option("C:\\Users\\output")
        case "prod" => Option("C:\\Users\\output")
        case _ => None
          throw new Exception(s"env name error: env name can only be test, stage or prod \ncannot be ${env()}")
      })

  val datePrefix: ScallopOption[String] =
    opt[String]("date.prefix",
      descr = "date prefix for path",
      required = false,
      default = Option("start_date"))

  val processDate: ScallopOption[String] =
    opt[String]("process.date",
      descr = "date to process data in YYYY-MM-DD format",
      required = true)

  val uniqueUsersPath: ScallopOption[String] =
    opt[String]("unique.user.path",
      descr = "path to save unique user id and join date",
      required = false,
      default = Option("C:\\Users\\bike\\unique-users"))

  val dayAgo: ScallopOption[Int] =
    opt[Int]("day.ago",
      descr = "data how many days ago you are writing back to",
      required = false,
      default = Option(1))

  verify()
}

