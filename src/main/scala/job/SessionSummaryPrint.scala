package job

import scopt.OParser

import org.apache.spark.sql.SparkSession

object SessionSummaryPrint {
  case class Config(
                     baseUrl: String = "",
                     execMode: String = "local",
                   )

  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[Config]
    val parser = {
      import builder._

      OParser.sequence(
        programName("SparkEtlPilot " + args(0)),
        opt[String]("dl-base")
          .required()
          .action((x, c) => c.copy(baseUrl = x))
          .text("Base url to datalake"),
        opt[String]("exec-mode")
          .action((x, c) => c.copy(execMode= x))
          .text("Spark execution mode"),
      )
    }

    OParser.parse(parser, args.drop(1), Config()) match {
      case Some(config) =>
        this.run(config)

      case None =>
      // print help
    }
  }

  private def run(config: Config): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SessionSummaryPrint")
      .master(config.execMode)
      .getOrCreate()

    import spark.implicits._

    val rawLogs = spark.read
      .parquet(config.baseUrl + "/transformation/SessionSummaryTrans/output/*")

    val total = rawLogs
    val loggedIn = rawLogs.where(!$"user_id".isNull)
    val users = rawLogs.groupBy("user_id").count()

    val t = total.count()
    val l = loggedIn.count()
    val u = users.count()

    println(t, l, u)
  }
}
