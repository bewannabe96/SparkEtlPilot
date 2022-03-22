import scopt.OParser

import job.{SessionSummaryPrint, SessionSummaryTrans}

object SparkEtlPilot {
  object Job extends Enumeration {
    type Job = Value
    val
    SessionSummaryTrans,
    SessionSummaryPrint
    = Value
  }
  implicit val jobRead: scopt.Read[Job.Value] = scopt.Read.reads(Job withName _)

  case class Config(
                     job: Job.Value = Job.SessionSummaryTrans,
                   )

  def main(args: Array[String]): Unit = {

    val builder = OParser.builder[Config]
    val parser = {
      import builder._

      OParser.sequence(
        programName("SparkEtlPilot"),
        arg[Job.Value]("<job>")
          .unbounded()
          .action((x, c) => c.copy(job = x))
          .text("Spark job to run"),
      )
    }

    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        config.job match {

          case Job.SessionSummaryTrans =>
            SessionSummaryTrans.main(args)

          case Job.SessionSummaryPrint =>
            SessionSummaryPrint.main(args)

        }

      case None =>
        // print help
    }
  }
}
