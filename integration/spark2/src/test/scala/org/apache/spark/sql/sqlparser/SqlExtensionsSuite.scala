package org.apache.spark.sql.sqlparser

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.{SparkSession, SQLConf}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.execution.strategy.{CarbonLateDecodeStrategy, DDLStrategy, StreamingTableStrategy}
import org.apache.spark.sql.hive.{CarbonIUDAnalysisRule, CarbonPreAggregateDataLoadingRules, CarbonPreAggregateQueryRules, CarbonPreInsertionCasts}
import org.apache.spark.sql.optimizer.{CarbonIUDRule, CarbonLateDecodeRule, CarbonUDFTransformRule}
import org.apache.spark.sql.parser.CarbonSparkSqlParser
import org.apache.spark.sql.test.util.PlanTest

class SqlExtensionsSuite extends PlanTest with BeforeAndAfterAll {

  var session: SparkSession = null

  val sparkCommands = Array("select 2 > 1")

  val carbonCommands = Array("show STREAMS")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    session = SparkSession
      .builder()
      .appName("parserApp")
      .master("local")
      .withExtensions(extensions => {

        // Carbon parser
        extensions
          .injectParser((sparkSession: SparkSession, _: ParserInterface) =>
            new CarbonSparkSqlParser(new SQLConf, sparkSession))

        // carbon analyzer rules
        extensions
          .injectResolutionRule((session: SparkSession) => CarbonIUDAnalysisRule(session))
        extensions
          .injectResolutionRule((session: SparkSession) => CarbonPreInsertionCasts(session))


        // carbon post adhoc resolution rules
        extensions
          .injectPostHocResolutionRule((session: SparkSession) => new CarbonPreAggregateDataLoadingRules(session))
        extensions
          .injectPostHocResolutionRule((session: SparkSession) => new CarbonPreAggregateQueryRules(session))

        // carbon extra optimizations
        extensions
          .injectOptimizerRule((_: SparkSession) => new CarbonIUDRule)
        extensions
          .injectOptimizerRule((_: SparkSession) => new CarbonUDFTransformRule)
        extensions
          .injectOptimizerRule((_: SparkSession) => new CarbonLateDecodeRule)

        // carbon planner strategies
        extensions
          .injectPlannerStrategy((session: SparkSession) => new StreamingTableStrategy(session))
        extensions
          .injectPlannerStrategy((_: SparkSession) => new CarbonLateDecodeStrategy)
        extensions
          .injectPlannerStrategy((session: SparkSession) => new DDLStrategy(session))

      })
      .getOrCreate()
  }

  test("test parser injection") {
    assert(session.sessionState.sqlParser.isInstanceOf[CarbonSparkSqlParser])
    (carbonCommands ++ sparkCommands) foreach (command =>
      session.sql(command).show)
  }

  test("test analyzer injection") {

  }

  test("test strategy injection") {
    assert(session.sessionState.planner.strategies.filter(_.isInstanceOf[DDLStrategy]).length == 1)
    session.sql("create table if not exists table1 (column1 String) using carbondata ").show
  }
}
