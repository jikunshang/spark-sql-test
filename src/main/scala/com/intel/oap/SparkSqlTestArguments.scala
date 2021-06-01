package com.intel.oap

import java.util.Locale

class SparkSqlTestArguments(val args: Array[String]) {
  var queryFilter: Set[String] = Set.empty
  var dbName: String = null
  var round: Int = 1
  var testType: String = ""
  var powerTest: Boolean = true
  var tpStreamNum: Int = 4
  var userCmd: String = ""
  // eg: userCmd = "sh /home/sparkuser/cmd_before_query.sh"

  parseArgs(args.toList)
  validateArguments()

  private def optionMatch(optionName: String, s: String): Boolean = {
    optionName == s.toLowerCase(Locale.ROOT)
  }

  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs

    while (args.nonEmpty) {
      args match {
        case optName :: value :: tail if optionMatch("--database-name", optName) =>
          dbName = value
          args = tail

        case optName :: value :: tail if optionMatch("--test-type", optName) =>
          testType = value
          args = tail

        case optName :: value :: tail if optionMatch("--query-filter", optName) =>
          queryFilter = value.toLowerCase(Locale.ROOT).split(",").map(_.trim).toSet
          args = tail

        case optName :: value :: tail if optionMatch("--round", optName) =>
          round = value.toInt
          args = tail

        case optName :: tail if optionMatch("--tp-test", optName) =>
          powerTest = false
          args = tail

        case optName :: value :: tail if optionMatch("--stream-num", optName) =>
          tpStreamNum = value.toInt
          args = tail

        case optName :: value :: tail if optionMatch("--user-cmd", optName) =>
          userCmd = value
          args = tail

        case _ =>
          // scalastyle:off println
          System.err.println("Unknown/unsupported param " + args)
          // scalastyle:on println
          printUsageAndExit(1)
      }
    }
  }

  private def printUsageAndExit(exitCode: Int): Unit = {
    // scalastyle:off
    System.err.println("""
        |Usage: spark-submit --class <this class> <spark sql test jar> [Options]
        |Options:
        |  --database-name      TPCDS/ TPCH database name
        |  --test-type          Run tpcds or tpch
        |  --query-filter       Queries to filter, e.g., q3,q5,q13
        |  --round              how many rounds to run, default is 1
        |  --tp-test            run throughput test, will use 4 streams by default, will shuffle queries.
        |  --stream-num         stream number for throughput test, default value is 4
        |  --user-cmd           cmd before each query, for example, you can clear cache.
        |
        |------------------------------------------------------------------------------------------------------------------
        |In order to run this benchmark, please follow the instructions at
        |https://github.com/databricks/spark-sql-perf/blob/master/README.md
        |to generate the TPCDS data locally (preferably with a scale factor of 5 for benchmarking).
      """.stripMargin)
    // scalastyle:on
    System.exit(exitCode)
  }

  private def validateArguments(): Unit = {
    if (dbName == null || testType == null) {
      // scalastyle:off println
      System.err.println("Must specify a database and a test type(tpcds or tpch)")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
  }
}

