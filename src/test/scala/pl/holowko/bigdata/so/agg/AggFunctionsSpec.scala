package pl.holowko.bigdata.so.agg

import com.github.nscala_time.time.Imports.{DateTime, Period, _}
import org.apache.spark.sql.Row
import org.scalatest._
import pl.holowko.bigdata.so.agg.functions._
import pl.holowko.bigdata.so.dataframe.R
import pl.holowko.bigdata.so.domain.Questions
import pl.holowko.bigdata.so.sparkspec.SparkSpec

import scala.language.postfixOps

class AggFunctionsSpec extends FlatSpec with SparkSpec with GivenWhenThen with Matchers with Assertions {

  val SCALA = "scala"
  val JAVA = "java"
  val HDFS = "hdfs"
  
  val JAN = "2017-01-01"
  val FEB = "2017-02-01"
  val MAR = "2017-03-01"
  
  val USER_1 = 128L
  val USER_2 = 512L
  val USER_3 = 1024L
  
  behavior of "AggFunctionsSpec" 
  
  it should "count views by tag" in {
    Given("questions dataset")
    val qDS = Questions.builder(spark)
      .tag(SCALA).viewCount(2).and()
      .tag(SCALA).viewCount(3).and()
      .tag(SCALA).viewCount(4).and()
      .tag(JAVA).viewCount(3).and()
      .tag(JAVA).viewCount(5).and()
      .ds()

    When("collect viewCountByTag")
    val rows: Array[Row] = qDS.viewCountByTag().collect()

    Then("result should contain aggregated data")
    rows should contain theSameElementsAs Seq(
      //|| tag   | viewCount ||
      R || SCALA |     9     ||,
      R || JAVA  |     8     ||
    )
  }
  
  it should "count views by tag and month" in {
    Given("questions dataset")
    val qDS = Questions.builder(spark)
      .tag(SCALA).month(JAN).viewCount(3).and()
      .tag(SCALA).month(JAN).viewCount(2).and()
      .tag(SCALA).month(JAN).viewCount(1).and()
      .tag(SCALA).month(FEB).viewCount(2).and()
      .tag(JAVA).month(JAN).viewCount(3).and()
      .tag(JAVA).month(FEB).and()
      .ds()

    When("collect viewCountByTagAndMonth")
    val rows: Array[Row] = qDS.viewCountByTagAndMonth().collect()

    Then("result should contain aggregated data")
    rows should contain theSameElementsAs Seq(
      //|| tag   | month | viewCount ||
      R || SCALA |  JAN  |     6     ||,
      R || SCALA |  FEB  |     2     ||,
      R || JAVA  |  JAN  |     3     ||,
      R || JAVA  |  FEB  |     0     ||
    )
  }
  
  it should "count by tag and month" in {
    Given("questions dataset")
    val qDS = Questions.builder(spark)
      .tags(Seq(SCALA, JAVA)).month(JAN).and()
      .tags(Seq(SCALA)).month(JAN).and()
      .tags(Seq(SCALA, JAVA)).month(FEB).and()
      .tags(Seq(JAVA)).month(FEB).and()
      .tags(Seq(JAVA, SCALA)).month(MAR).and()
      .tags(Seq(JAVA, SCALA)).month(MAR).and()
      .tags(Seq(JAVA, SCALA)).month(MAR).and()
      .tags(Seq(JAVA)).month(MAR).and()
      .ds()

    When("collect countByTagAndMonth")
    val rows: Array[Row] = qDS.countByTagAndMonth().collect()

    Then("result should contain aggregated data")
    rows should contain theSameElementsAs Seq(
      //|| tag   | month | count ||
      R || SCALA |  JAN  |   2   ||,
      R || SCALA |  FEB  |   1   ||,
      R || SCALA |  MAR  |   3   ||,
      R || JAVA  |  JAN  |   1   ||,
      R || JAVA  |  FEB  |   2   ||,
      R || JAVA  |  MAR  |   4   ||
    )
  }
  
  it should "count by month" in {
    Given("questions dataset")
    val qDS = Questions.builder(spark)
      .tags(Seq(SCALA, JAVA, HDFS)).month(JAN).and()
      .tags(Seq(SCALA, HDFS)).month(JAN).and()
      .tags(Seq(SCALA, JAVA)).month(FEB).and()
      .tags(Seq(JAVA, SCALA)).month(MAR).and()
      .tags(Seq(JAVA)).month(MAR).and()
      .ds()

    When("collect countByTagAndMonth")
    val rows: Array[Row] = qDS.countByMonth().collect()

    Then("result should contain aggregated data")
    rows should contain theSameElementsAs Seq(
      //|| month | count ||
      R ||  JAN  |   5   ||,
      R ||  FEB  |   2   ||,
      R ||  MAR  |   3   ||
    )
  }

  it should "count by tag" in {
    Given("questions dataset")
    val qDS = Questions.builder(spark)
      .tag(SCALA).and()
      .tag(SCALA).and()
      .tag(JAVA).and()
      .tag(JAVA).and()
      .tag(JAVA).and()
      .ds()

    When("collect questionCountByTag")
    val rows: Array[Row] = qDS.countByTag().collect()

    Then("result should contain aggregated data")
    rows should contain theSameElementsAs Seq(
      //|| tag   | questionCount ||
      R || SCALA |       2       ||,
      R || JAVA  |       3       ||
    )
  }

  it should "count answers by tag" in {
    Given("questions dataset")
    val qDS = Questions.builder(spark)
      .tag(SCALA).answerCount(2).and()
      .tag(SCALA).answerCount(4).and()
      .tag(JAVA).answerCount(5).and()
      .tag(JAVA).answerCount(1).and()
      .tag(JAVA).answerCount(3).and()
      .ds()

    When("collect answerCountByTag")
    val rows: Array[Row] = qDS.answerCountByTag().collect()

    Then("result should contain aggregated data")
    rows should contain theSameElementsAs Seq(
      //|| tag   | answerCount ||
      R || SCALA |     6       ||,
      R || JAVA  |     9       ||
    )
  }

  it should "count accepted answers by tag" in {
    Given("questions dataset")
    val qDS = Questions.builder(spark)
      .tag(SCALA).acceptedAnswerId(1).and()
      .tag(SCALA).acceptedAnswerId(4).and()
      .tag(SCALA).answerCount(100).and()
      .tag(JAVA).acceptedAnswerId(5).and()
      .tag(JAVA).acceptedAnswerId(1).and()
      .tag(JAVA).acceptedAnswerId(3).and()
      .tag(JAVA).answerCount(100).and()
      .tag(JAVA).and()
      .ds()

    When("collect acceptedAnswerCountByTag")
    val rows: Array[Row] = qDS.acceptedAnswerCountByTag().collect()

    Then("result should contain aggregated data")
    rows should contain theSameElementsAs Seq(
      //|| tag   | acceptedAnswerCount ||
      R || SCALA |          2          ||,
      R || JAVA  |          3          ||
    )
  }

  it should "count avg answers by tag" in {
    Given("questions dataset")
    val qDS = Questions.builder(spark)
      .tag(SCALA).answerCount(2).and()
      .tag(SCALA).answerCount(4).and()
      .tag(JAVA).answerCount(6).and()
      .tag(JAVA).answerCount(3).and()
      .tag(JAVA).answerCount(6).and()
      .tag(JAVA).answerCount(5).and()
      .ds()

    When("collect avgAnswerCountByTag")
    val rows: Array[Row] = qDS.avgAnswerCountByTag().collect()

    Then("result should contain aggregated data")
    rows should contain theSameElementsAs Seq(
      //|| tag   | avgAnswerCount ||
      R || SCALA |     3       ||,
      R || JAVA  |     5       ||
    )
  }

  it should "count questions and answers by tag and month" in {
    Given("questions dataset")
    val qDS = Questions.builder(spark)
      .tag(SCALA).answerCount(2).month(JAN).and()
      .tag(SCALA).answerCount(3).month(FEB).and()
      .tag(SCALA).answerCount(4).month(FEB).and()
      .tag(JAVA).answerCount(5).month(JAN).and()
      .tag(JAVA).answerCount(1).month(JAN).and()
      .tag(JAVA).answerCount(3).month(FEB).and()
      .ds()

    When("collect questionAnswerCountByTagAndMonth")
    val rows: Array[Row] = qDS.answerCountByTagAndMonth().collect()

    Then("result should contain aggregated data")
    rows should contain theSameElementsAs Seq(
      //||  tag  | month | questionCount | answerCount || 
      R || SCALA |  JAN  |       1       |      2      ||,
      R || SCALA |  FEB  |       2       |      7      ||,
      R || JAVA  |  JAN  |       2       |      6      ||,
      R || JAVA  |  FEB  |       1       |      3      ||
    )
  }

  it should "count questions with answers by tag and month" in {
    Given("questions dataset")
    val qDS = Questions.builder(spark)
      .tag(SCALA).month(JAN).answerCount(2).and()
      .tag(SCALA).month(JAN).and()
      .tag(SCALA).month(FEB).answerCount(3).and()
      .tag(SCALA).month(FEB).answerCount(4).and()
      .tag(SCALA).month(FEB).and()
      .tag(JAVA).month(JAN).and()
      .tag(JAVA).month(JAN).and()
      .tag(JAVA).month(FEB).answerCount(1).and()
      .ds()

    When("collect questionWithAnswerCountByTagAndMonth")
    val rows: Array[Row] = qDS.questionWithAnswerCountByTagAndMonth().collect()

    Then("result should contain aggregated data")
    rows should contain theSameElementsAs Seq(
      //||  tag  | month | questionWithAnswerCount || 
      R || SCALA |  JAN  | 1 ||,
      R || SCALA |  FEB  | 2 ||,
      R || JAVA  |  JAN  | 0 ||,
      R || JAVA  |  FEB  | 1 ||
    )
  }

  //tag::allStatsByTagAndMonthTest[]
  it should "create stats for questions by tag and month" in {
    Given("questions dataset")
    val qDS = Questions.builder(spark) //<1>
      .tag(SCALA).month(JAN).answerCount(2).acceptedAnswerId(1).viewCount(3).and()
      .tag(SCALA).month(JAN).viewCount(2).and()
      .tag(SCALA).month(FEB).answerCount(3).and()
      .tag(SCALA).month(FEB).answerCount(4).viewCount(3).and()
      .tag(SCALA).month(FEB).and()
      .tag(JAVA).month(JAN).viewCount(1).and()
      .tag(JAVA).month(JAN).viewCount(1).and()
      .tag(JAVA).month(JAN).and()
      .tag(JAVA).month(FEB).answerCount(1).acceptedAnswerId(2).and()
      .ds()

    When("collect allStatsByTagAndMonth")
    val rows: Array[Row] = qDS.allStatsByTagAndMonth().toDF().collect()

    Then("result should contain aggregated data")
    rows should contain theSameElementsAs Seq(
      //|| tag   | month | question | answer | questionWith | questionWithout | questionWith        | viewCount||
      //||       |       | count    | count  | AnswerCount  | AnswerCount     | AcceptedAnswerCount |          ||
      R || SCALA |  JAN  |     2    |   2    |      1       |        1        |          1          |     5    ||, //<2>
      R || SCALA |  FEB  |     3    |   7    |      2       |        1        |          0          |     3    ||,
      R || JAVA  |  JAN  |     3    |   0    |      0       |        3        |          0          |     2    ||,
      R || JAVA  |  FEB  |     1    |   1    |      1       |        0        |          1          |     0    ||
    )
  }
  //end::allStatsByTagAndMonthTest[]

  it should "create stats for questions by tag" in {
    Given("questions dataset")
    val qDS = Questions.builder(spark)
      .tag(SCALA).month(JAN).answerCount(2).acceptedAnswerId(1).viewCount(3).and()
      .tag(SCALA).month(JAN).viewCount(2).and()
      .tag(SCALA).month(FEB).answerCount(3).and()
      .tag(SCALA).month(FEB).answerCount(4).viewCount(3).and()
      .tag(SCALA).month(FEB).and()
      .tag(JAVA).month(JAN).viewCount(1).and()
      .tag(JAVA).month(JAN).viewCount(1).and()
      .tag(JAVA).month(JAN).and()
      .tag(JAVA).month(FEB).answerCount(1).acceptedAnswerId(2).and()
      .ds()

    When("collect allStatsByTagAndMonth")
    val rows: Array[Row] = qDS.allStatsByTagAndMonth()
      .groupByTag().toDF().collect()

    Then("result should contain aggregated data")
    rows should contain theSameElementsAs Seq(
      //|| tag   | question | answer | questionWith | questionWithout | questionWith        | viewCount ||
      //||       | count    | count  | AnswerCount  | AnswerCount     | AcceptedAnswerCount |           ||
      R || SCALA |     5    |   9    |      3       |        2        |          1          |     8     ||,
      R || JAVA  |     4    |   1    |      1       |        3        |          1          |     2     ||
    )
  }
  
  it should "create stats for questions by tag in percentege" in {
    Given("questions dataset")
    val qDS = Questions.builder(spark)
      .tag(SCALA).month(JAN).answerCount(2).acceptedAnswerId(1).viewCount(3).and()
      .tag(SCALA).month(JAN).viewCount(2).and()
      .tag(SCALA).month(FEB).answerCount(3).viewCount(3).and()
      .tag(SCALA).month(FEB).and()
      .tag(SCALA).month(FEB).and()
      .tag(SCALA).month(FEB).and()
      .tag(JAVA).month(JAN).viewCount(1).and()
      .tag(JAVA).month(JAN).viewCount(1).and()
      .tag(JAVA).month(FEB).answerCount(1).and()
      .tag(JAVA).month(FEB).answerCount(1).acceptedAnswerId(2).and()
      .ds()

    When("collect allStatsByTagAndMonth().allStatsByTag().inPercentage()")
    val rows: Array[Row] = qDS.allStatsByTagAndMonth()
      .groupByTag()
      .inPercentage()
      .toDF()
      .collect()

    Then("result should contain aggregated data")
    rows should contain theSameElementsAs Seq(
      //|| tag   | question | questionWith | questionWithout | questionWith        | viewCount ||
      //||       | count    | AnswerCount  | AnswerCount     | AcceptedAnswerCount |           ||
      R || SCALA |   60.0   |     33.33    |      66.67      |        16.67        |    80.0   ||,
      R || JAVA  |   40.0   |     50.0     |      50.0       |        25.0         |    20.0   ||
    )
  }
  
  it should "create stats for questions by tag in month with filled gaps" in {
    Given("questions dataset")
    val qDS = Questions.builder(spark)
      .tag(SCALA).month(JAN).answerCount(2).acceptedAnswerId(1).and()
      .tag(SCALA).month(JAN).viewCount(3).and()
      .tag(JAVA).month(MAR).answerCount(1).acceptedAnswerId(2).and()
      .ds()

    When("collect allStatsByTagAndMonth")
    val rows: Array[Row] = qDS.allStatsByTagAndMonth()
      .fillMonthGaps()
      .toDF()
      .collect()

    Then("result should contain aggregated data")
     rows should contain theSameElementsAs Seq(
      //|| tag   | month | question | answer | questionWith | questionWithout | questionWith        | viewCount ||
      //||       |       | count    | count  | AnswerCount  | AnswerCount     | AcceptedAnswerCount |           ||
      R || SCALA |  JAN  |     2    |   2    |      1       |        1        |          1          |     3     ||,
      R || SCALA |  FEB  |     0    |   0    |      0       |        0        |          0          |     0     ||,
      R || SCALA |  MAR  |     0    |   0    |      0       |        0        |          0          |     0     ||,
      R || JAVA  |  JAN  |     0    |   0    |      0       |        0        |          0          |     0     ||,
      R || JAVA  |  FEB  |     0    |   0    |      0       |        0        |          0          |     0     ||,
      R || JAVA  |  MAR  |     1    |   1    |      1       |        0        |          1          |     0     ||
    )
  }

  it should "create stats for questions by tag and month in percentage" in {
    Given("questions dataset")
    val qDS = Questions.builder(spark)
      .tag(SCALA).month(JAN).answerCount(2).acceptedAnswerId(1).and()
      .tag(SCALA).month(JAN).and()
      .tag(JAVA).month(JAN).and()
      .tag(JAVA).month(JAN).and()
      .ds()

    When("collect allStatsByTagAndMonth")
    val rows: Array[Row] = qDS.allStatsByTagAndMonth()
      .inPercentage()
      .collect()

    Then("result should contain aggregated data")
    rows should contain theSameElementsAs Seq(
      //|| tag   | month | questions | questionWithAnswer | questionWithoutAnswer | questionWithAccepted ||
      //||       |       | AndMonth  | CountAndMonth      | CountAndMonth         | AnswerCountAndMonth  ||
      R || SCALA |  JAN  |   50.0    |       50.0         |         50.0          |         50.0         ||,
      R || JAVA  |  JAN  |   50.0    |        0.0         |        100.0          |          0.0         ||
    )
  }
  
  it should "count avg time to first answer by tag" in {
    Given("questions dataset")
    val day = DateTime.parse("2017-01-01")
    val questionBuilder = Questions.builder(spark)
      .tags(Seq(SCALA, HDFS)).time(day)
        .withAnswer(day + 2.hours)
        .withAnswer(day + 1.hour).and()
      .tag(SCALA).time(day)
        .withAnswer(day + 3.hours)
        .withAnswer(day + 4.hours).and()
      .tag(SCALA).time(day).and()
      .tag(JAVA).time(day).and()
      .tag(JAVA).time(day).and()
    val qDS = questionBuilder.questionsDS()
    val aDS = questionBuilder.answersDS()

    When("collect timeToFirstAnswer avgMinByTag")
    val rows: Array[Row] = qDS.timeToFirstAnswer(aDS).avgMinByTag().collect()

    Then("result should contain aggregated data")
    rows should contain theSameElementsAs Seq(
      //|| tag   | avgTimeToAnswerMs | minTimeToAnswerMs | avgHuman  | minHuman  ||
      R || SCALA |     ms(2 hours)   |     ms(1 hour)    | "2 hours" | "1 hour"  ||,
      R || HDFS  |     ms(1 hour)    |     ms(1 hour)    | "1 hour"  | "1 hour"  ||
    )
  }
  
  it should "count avg time to accepted answer by tag" in {
    Given("questions dataset")
    val day = DateTime.parse("2017-01-01")
    val questionBuilder = Questions.builder(spark)
      .tags(Seq(SCALA, HDFS)).time(day)
        .withAcceptedAnswer(day + 2.hours)
        .withAnswer(day + 1.hour).and()
      .tag(SCALA).time(day)
        .withAnswer(day + 3.hours)
        .withAcceptedAnswer(day + 4.hours).and()
      .tag(SCALA).time(day).and()
      .tag(JAVA).time(day).and()
      .tag(JAVA).time(day).and()
    val qDS = questionBuilder.questionsDS()
    val aDS = questionBuilder.answersDS()

    When("collect timeToAcceptedAnswer avgMinByTag")
    val rows: Array[Row] = qDS.timeToAcceptedAnswer(aDS).avgMinByTag().collect()

    Then("result should contain aggregated data")
    rows should contain theSameElementsAs Seq(
      //|| tag   | avgTimeToAnswerMs | minTimeToAnswerMs | avgHuman  | minHuman  ||
      R || SCALA |     ms(3 hours)   |     ms(2 hour)    | "3 hours" | "2 hours" ||,
      R || HDFS  |     ms(2 hour)    |     ms(2 hour)    | "2 hours" | "2 hours" ||
    )
  }
  
//  it should "count big avg time to accepted answer by tag" in {
//    Given("questions dataset")
//    val day = DateTime.parse("2017-01-01")
//    val questionBuilder = Questions.builder(spark)
//      .tag(HDFS).time(day)
//        .withAcceptedAnswer(day + 1024.days)
//        .withAnswer(day + 2.years).and()
//    val qDS = questionBuilder.questionsDS()
//    val aDS = questionBuilder.answersDS()
//
//    When("collect timeToAcceptedAnswer avgMinByTag")
//    val rows: Array[Row] = qDS.timeToAcceptedAnswer(aDS).avgMinByTag().collect()
//
//    Then("result should contain aggregated data")
//    rows should contain theSameElementsAs Seq(
//      //|| tag   | avgTimeToAnswerMs | minTimeToAnswerMs | avgHuman  | minHuman  ||
//      R || HDFS  |     ms(2 years)    |     ms(1024 days)    | "2 years" | "4 years" ||
//    )
//  }
  
  it should "count avg time to first answer" in {
    Given("questions dataset")
    val day = DateTime.parse("2017-01-01")
    val questionBuilder = Questions.builder(spark)
      .tags(Seq(SCALA, HDFS)).time(day)
        .withAcceptedAnswer(day + 2.hours)
        .withAnswer(day + 1.hour).and()
      .tag(SCALA).time(day)
        .withAnswer(day + 4.hours)
        .withAcceptedAnswer(day + 5.hours).and()
      .tag(SCALA).time(day).and()
      .tag(JAVA).time(day).and()
      .tag(JAVA).time(day).and()
    val qDS = questionBuilder.questionsDS()
    val aDS = questionBuilder.answersDS()

    When("collect timeToFirstAnswer average")
    val rows: Array[Row] = qDS.timeToFirstAnswer(aDS).average().collect()

    Then("result should contain aggregated data")
    rows should contain theSameElementsAs Seq(
      //|| avgTimeToAnswerMs | avgHuman  ||
      R ||     ms(2 hours)   | "2 hours" ||
    )
  }
  
  it should "count avg time to accepted answer" in {
    Given("questions dataset")
    val day = DateTime.parse("2017-01-01")
    val questionBuilder = Questions.builder(spark)
      .tags(Seq(SCALA, HDFS)).time(day)
        .withAcceptedAnswer(day + 2.hours)
        .withAnswer(day + 1.hour).and()
      .tag(SCALA).time(day)
        .withAnswer(day + 3.hours)
        .withAcceptedAnswer(day + 5.hours).and()
      .tag(SCALA).time(day).and()
      .tag(JAVA).time(day).and()
      .tag(JAVA).time(day).and()
    val qDS = questionBuilder.questionsDS()
    val aDS = questionBuilder.answersDS()

    When("collect timeToAcceptedAnswer average")
    val rows: Array[Row] = qDS.timeToAcceptedAnswer(aDS).average().collect()

    Then("result should contain aggregated data")
    rows should contain theSameElementsAs Seq(
      //|| avgTimeToAnswerMs | avgHuman  ||
      R ||     ms(3 hours)   | "3 hours" ||
    )
  }
  
  it should "count accepted Answer By Tag and UserId" in {
    Given("questions dataset")
    val questionBuilder = Questions.builder(spark)
      .tags(Seq(SCALA, HDFS)).withAcceptedAnswer(USER_1).and()
      .tags(Seq(JAVA, HDFS, SCALA)).withAcceptedAnswer(USER_1).and()
      .tag(SCALA).withAcceptedAnswer(USER_1).and()
      .tag(JAVA).withAcceptedAnswer(USER_2).and()
      .tag(HDFS).and()
      .tag(SCALA).and()
    val qDS = questionBuilder.questionsDS()
    val aDS = questionBuilder.answersDS()

    When("collect acceptedAnswerCountByTagUserId")
    val rows: Array[Row] = qDS.acceptedAnswerCountByTagAndUserId(aDS)
      .toDF().collect()

    Then("result should contain aggregated data")
    rows should contain theSameElementsAs Seq(
      //||  tag  |userId | acceptedAnswerCount ||
      R || SCALA | USER_1 |         3           ||,
      R || HDFS  | USER_1 |         2           ||,
      R || JAVA  | USER_1 |         1           ||,
      R || JAVA  | USER_2 |         1           ||
    )
  }
  
  it should "count users who accepted Answer By Tag" in {
    Given("questions dataset")
    val questionBuilder = Questions.builder(spark)
      .tags(Seq(SCALA, HDFS)).withAcceptedAnswer(USER_1).and()
      .tags(Seq(JAVA, HDFS, SCALA)).withAcceptedAnswer(USER_1).and()
      .tag(SCALA).withAcceptedAnswer(USER_1).and()
      .tag(SCALA).withAcceptedAnswer(USER_1).and()
      .tag(SCALA).withAcceptedAnswer(USER_2).and()
      .tag(SCALA).withAcceptedAnswer(USER_3).and()
      .tag(JAVA).withAcceptedAnswer(USER_2).and()
      .tag(HDFS).and()
      .tag(SCALA).and()
    val qDS = questionBuilder.questionsDS()
    val aDS = questionBuilder.answersDS()

    When("collect acceptedAnswerCountByTagUserId")
    val rows: Array[Row] = qDS.acceptedAnswerCountByTagAndUserId(aDS)
      .userCountByTag()
      .collect()

    Then("result should contain aggregated data")
    rows should contain theSameElementsAs Seq(
      //||  tag  | userCount ||
      R || SCALA |     3     ||,
      R || HDFS  |     1     ||,
      R || JAVA  |     2     ||
    )
  }
  
  it should "count answers By Tag and userId" in {
    Given("questions dataset")
    val questionBuilder = Questions.builder(spark)
      .tags(Seq(SCALA, HDFS)).withAnswer(USER_1).and()
      .tags(Seq(JAVA, HDFS, SCALA)).withAnswer(USER_1).and()
      .tag(SCALA).withAnswer(USER_1).and()
      .tag(SCALA).withAnswer(USER_1).and()
      .tag(SCALA).withAnswer(USER_2).and()
      .tag(SCALA).withAnswer(USER_3).and()
      .tag(JAVA).withAnswer(USER_1).and()
      .tag(JAVA).withAnswerAndNullUserId().and()
      .tag(HDFS).and()
      .tag(SCALA).and()
    val qDS = questionBuilder.questionsDS()
    val aDS = questionBuilder.answersDS()

    When("collect answerCountByTagAndUserId")
    val rows: Array[Row] = qDS.answerCountByTagAndUserId(aDS)
      .userCountByTag()
      .collect()

    Then("result should contain aggregated data")
    rows should contain theSameElementsAs Seq(
      //||  tag  | userCount ||
      R || SCALA |     3     ||,
      R || HDFS  |     1     ||,
      R || JAVA  |     1     ||
    )
  }
  
  def ms (period:Period):Long = {
    period.toStandardDuration.getMillis
  }

}
