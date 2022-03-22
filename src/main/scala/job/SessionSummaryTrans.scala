package job

import scopt.OParser

import org.apache.spark.sql.SparkSession

object SessionSummaryTrans {
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
      .appName("SessionSummaryTrans")
      .master(config.execMode)
      .getOrCreate()

    import spark.implicits._

    val rawLogs = spark.read.json(config.baseUrl + "/carving/log/2022/03/*/*")

    val sessionUsers = rawLogs
      .as("U")
      .select($"session_id", $"user_id")
      .where($"type" === "session_user")

    val sessions = rawLogs
      .as("S")
      .select(
        $"timestamp",
        $"session_id",
        $"address",
        $"latitude",
        $"longitude",
        $"country",
        $"country_region",
        $"timezone",
        $"locale",
        $"version",
        $"build_no",
        $"platform",
      )
      .where($"type" === "session_start")

    sessions
      .join(sessionUsers, $"S.session_id" === $"U.session_id", "left_outer")
      .select($"S.*", $"U.user_id")
      .write
      .parquet(config.baseUrl + "/transformation/SessionSummaryTrans/output")
  }
}
