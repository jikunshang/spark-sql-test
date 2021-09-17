package com.intel.oap

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.resourceToString
import scala.sys.process._

import scala.util.Random

object SparkSqlTest {
  def main(args: Array[String]) {
    // get arguments.
    val benchmarkArgs = new SparkSqlTestArguments(args)
    val dbName = benchmarkArgs.dbName
    val query_filter = benchmarkArgs.queryFilter
    val testType = benchmarkArgs.testType
    val powerTest: Boolean = benchmarkArgs.powerTest
    val threadNum = if (powerTest) 1 else benchmarkArgs.tpStreamNum
    val round = if (powerTest) benchmarkArgs.round else 1
    val cmdBeforeEachQuery = benchmarkArgs.userCmd

    val queriesToRun = if (testType == "tpcds") filterQueries(tpcdsQueriesLists, benchmarkArgs)
    else if (testType == "tpch") filterQueries(tpchQueriesLists, benchmarkArgs)
    else throw new UnsupportedOperationException("unsupported test type, accept 'tpcds' or 'tpch' only.")

    // build spark
    val conf = new SparkConf().setAppName("SparkSqlPowerTest")
    val spark = SparkSession.builder().enableHiveSupport()
      .config(conf)
      .getOrCreate()

    // use database
    spark.sql(s"use $dbName ;").collect()

    val start = System.nanoTime()
    val threadArray = new Array[Thread](threadNum)
    for (i <- 0 until threadNum) {
      val t = new TestThread(i, spark, powerTest, queriesToRun, testType, round, cmdBeforeEachQuery)
      threadArray(i) = new Thread(t)
      threadArray(i).start()
    }

    for (i <- 0 until threadNum) {
      threadArray(i).join()
    }

    val duration = (System.nanoTime() - start) / 1000 / 1000
    System.out.println(s"total execution time: $duration ms.")

  }

  // queryLocation is in resource dir, accept "tpcds" and "tpch" only
  class TestThread(threadID: Int, spark: SparkSession, powerTest: Boolean, qList: Seq[String],
                   queryLocation: String, round: Int, userCmd: String = "") extends Runnable {

    override def run(): Unit = {
      val runList = if (powerTest) qList else Random.shuffle(qList)
      //      System.out.println(runList.mkString(","))
      val resultTotal: Array[Array[Long]] = new Array(round)
      for (i <- 0 until round) {
        val resultPerRound: Array[Long] = new Array(runList.size)
        runList.zipWithIndex.foreach {
          case (name, index) =>
            val queryStr = resourceToString(s"$queryLocation/$name.sql",
              classLoader = Thread.currentThread().getContextClassLoader)
            try {
              val result = userCmd.!!
            }
            catch {
              case _ =>
            }
            val start = System.nanoTime()
            try {
              val df = spark.sql(queryStr)
              val res = df.collect()
              val durationInMs = (System.nanoTime() - start) / 1000 / 1000
              resultPerRound(index) = durationInMs
              System.out.println(s"round $i $name return rows: " + res.size + " result: " + res.mkString)

              val row = df.selectExpr(s"sum(crc32(concat_ws(',', *)))").head()
              val hash = if (row.isNullAt(0)) 0 else row.getLong(0)
              System.out.println(s"round $i $name result hash: " + hash)
            } catch {
              case _ => {
                System.out.println(s"query $name failed.")
                resultPerRound(index) = -1
              }
            }

          // output result for diff
          // System.out.println(s"round $i $name result: " + res.mkString)
          // System.out.println(s"thread $threadID run $name takes $durationInMs ms.")
        }
        resultTotal(i) = resultPerRound
      }
      System.out.println(s"Stream thread id: $threadID result")
      System.out.println(runList.mkString(","))
      resultTotal.foreach(res => System.out.println(res.mkString(",")))
      System.out.println()
    }
  }

  def filterQueries(origQueries: Seq[String],
                    args: SparkSqlTestArguments): Seq[String] = {
    if (args.queryFilter.nonEmpty) {
      origQueries.filter(args.queryFilter.contains)
    } else {
      origQueries
    }
  }

  val tpchQueriesLists = Seq(
    "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10",
    "q11", "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20",
    "q21", "q22")

  val tpcdsQueriesLists = Seq(
    "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
    "q12", "q13", "q14a", "q14b", "q15", "q16", "q17", "q18", "q19", "q20",
    "q21", "q22", "q23a", "q23b", "q24a", "q24b", "q25", "q26", "q27", "q28", "q29", "q30",
    "q31", "q32", "q33", "q34", "q35", "q36", "q37", "q38", "q39a", "q39b", "q40",
    "q41", "q42", "q43", "q44", "q45", "q46", "q47", "q48", "q49", "q50",
    "q51", "q52", "q53", "q54", "q55", "q56", "q57", "q58", "q59", "q60",
    "q61", "q62", "q63", "q64", "q65", "q66", "q67", "q68", "q69", "q70",
    "q71", "q72", "q73", "q74", "q75", "q76", "q77", "q78", "q79", "q80",
    "q81", "q82", "q83", "q84", "q85", "q86", "q87", "q88", "q89", "q90",
    "q91", "q92", "q93", "q94", "q95", "q96", "q97", "q98", "q99")


}