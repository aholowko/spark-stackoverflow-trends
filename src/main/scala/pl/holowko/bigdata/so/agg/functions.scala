package pl.holowko.bigdata.so.agg

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{min, _}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.joda.time.format.PeriodFormat
import pl.holowko.bigdata.so.domain.columns.{tag, _}
import pl.holowko.bigdata.so.domain.{Answer, Question}

object functions {

  private val _humanMs: Long => String = m =>
    PeriodFormat.wordBased().print(
      Period.millis(m.toInt).normalizedStandard()
    )
  
  private val humanMs: UserDefinedFunction = udf(_humanMs)
  
  implicit class QuestionDataset(questions:Dataset[Question]) {
    
    def writeToOneCsv(path:String): Unit = {
      questions
      .coalesce(1)
      .write
      .option("header", value = true)
      .csv(path)
    }
    
    // tag::viewCountByTag[]
    def viewCountByTag()(implicit spark:SparkSession): DataFrame = {
      import spark.implicits._
      questions
        .groupBy(tag)
        .agg(
          sum(viewCount) as viewCount
        )
    }
    // end::viewCountByTag[]
    
    def viewCountByTagAndMonth()(implicit spark:SparkSession): DataFrame = {
      import spark.implicits._
      questions
        .groupBy(tag, monthCol)
        .agg(
          sum(viewCount) as viewCount
        )
        .sort(tag.asc, monthCol.asc)
    }
    
    def countByTag()(implicit spark:SparkSession): DataFrame = {
      import spark.implicits._
      questions
        .groupBy(tag)
        .agg(
          count(tag) as countAgg
        )
    }
    
    def countByTagAndMonth()(implicit spark:SparkSession): DataFrame = {
      import spark.implicits._
      questions
        .groupBy(tag, monthCol)
        .agg(
          count(tag) as countAgg
        )
    }
    
    def countByMonth()(implicit spark:SparkSession): DataFrame = {
      import spark.implicits._
      questions
        .groupBy(monthCol)
        .agg(
          count(monthCol) as countAgg
        )
    }
    
    def answerCountByTag()(implicit spark:SparkSession): DataFrame = {
      import spark.implicits._
      questions
        .groupBy(tag)
        .agg(
          sum(answerCount) as answerCount
        )
        .sort(answerCount.desc)
    }
    
    def acceptedAnswerCountByTag()(implicit spark:SparkSession): DataFrame = {
      import spark.implicits._
      questions
        .filter(_.acceptedAnswerId.isDefined)
        .groupBy(tag)
        .agg(
          count("*") as countAgg
        )
        .sort(countAgg.desc)
    }
    
    def avgAnswerCountByTag()(implicit spark:SparkSession): DataFrame = {
      import spark.implicits._
      questions
        .groupBy(tag)
        .agg(
          avg(answerCount) as avgAnswerCount
        )
        .sort(avgAnswerCount.desc)
    }
    
    def answerCountByTagAndMonth()(implicit spark:SparkSession): DataFrame = {
      import spark.implicits._
      questions
        .groupBy(tag, monthCol)
        .agg(
          count(tag) as questionCount,
          sum(answerCount) as answerCount
        )
    }
    
    def questionWithAnswerCountByTagAndMonth()(implicit spark:SparkSession): DataFrame = {
      import spark.implicits._
      questions
        .groupBy(tag, monthCol)
        .agg(
          sum(
            when(answerCount > 0, 1)
            .otherwise(0)
          ) as questionWithAnswerCount
        )
    }
    
    //tag::allStatsByTagAndMonth[]
    def allStatsByTagAndMonth()(implicit spark:SparkSession): Dataset[TagAndMonthStats] = {
      import spark.implicits._
      questions
        .groupBy(tag, monthCol)                   //<1>
        .agg(
          count(tag)            as questionCount, //<2> 
          sum(answerCount)      as answerCount,   //<3>
          sum(                                    //<4>
            when(answerCount > 0, 1)
            .otherwise(0)
          )                     as questionWithAnswerCount,
          sum(                                    //<5>
            when(answerCount === 0, 1)
            .otherwise(0)
          )                     as questionWithoutAnswerCount,
          sum(                                    //<6>
            when(acceptedAnswerId.isNotNull, 1)
            .otherwise(0)
          )                     as questionWithAcceptedAnswerCount,
          sum(viewCount)        as viewCount       //<7>
        )
        .as[TagAndMonthStats]
    }
    //end::allStatsByTagAndMonth[]
    
    def timeToFirstAnswer(answers:Dataset[Answer])(implicit spark:SparkSession): Dataset[TagTime] = {
      import spark.implicits._
      questions
        .filter(_.answerCount > 0)
        .join(answers, qid === parentId)
        .groupBy(qid, tag)
        .agg(
          min(aTimestamp - qTimestamp) as timeMs
        )
        .as[TagTime]
    }

    def timeToAcceptedAnswer(answers:Dataset[Answer])(implicit spark:SparkSession): Dataset[TagTime] = {
      import spark.implicits._
      questions
        .filter(_.acceptedAnswerId.isDefined)
        .join(answers, acceptedAnswerId === aid)
        .groupBy(qid, tag)
        .agg(
          min(aTimestamp - qTimestamp) as timeMs
        )
        .as[TagTime]
    }
    
    //tag::acceptedAnswerCountByTagAndUserId[]
    def acceptedAnswerCountByTagAndUserId(answers:Dataset[Answer])(implicit spark:SparkSession): Dataset[TagUserAnswerCount] = {
      import spark.implicits._
      questions
        .filter(_.acceptedAnswerId.isDefined) //<1>
        .join(answers, acceptedAnswerId === aid) //<2>
        .groupBy(tag, aOwnerUserId) //<3>
        .agg(count("*") as answerCount) //<4>
        .sort(answerCount.desc) //<5>
        .as[TagUserAnswerCount]
    }
    //end::acceptedAnswerCountByTagAndUserId[]
    
    //tag::answerCountByTagAndUserId[]
    def answerCountByTagAndUserId(answers:Dataset[Answer])(implicit spark:SparkSession): Dataset[TagUserAnswerCount] = {
      import spark.implicits._
      
      val answersWithUserId = answers
        .filter(aOwnerUserId.isNotNull)
      
      questions
        .filter(_.answerCount > 0) //<1>
        .join(answersWithUserId, qid === parentId) //<2>
        .groupBy(tag, aOwnerUserId) //<3>
        .agg(count("*") as answerCount) //<4>
        .sort(answerCount.desc) //<5>
        .as[TagUserAnswerCount]
    }
    //end::answerCountByTagAndUserId[]
    
  }
  
  implicit class TagAndMonthStatsDataset(stats:Dataset[TagAndMonthStats]) {

    //tag::groupByTag[]
    def groupByTag()(implicit spark: SparkSession): Dataset[TagStats] = {
      import spark.implicits._
      stats
        .groupBy(tag) //<1>
        .agg( //<2>
          sum(questionCount) as questionCount,
          sum(answerCount) as answerCount,
          sum(questionWithAnswerCount) as questionWithAnswerCount,
          sum(questionWithoutAnswerCount) as questionWithoutAnswerCount,
          sum(questionWithAcceptedAnswerCount) as questionWithAcceptedAnswerCount,
          sum(viewCount) as viewCount
        ).as[TagStats]
    }
    //end::groupByTag[]

    //tag::inPercentage[]
    def inPercentage()(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._

      val countAllInMonthByMonth = stats
        .groupBy(monthCol as monthCol2)
        .agg(
          sum(questionCount) as countAllInMonth //<1>
        )

      stats.join(countAllInMonthByMonth, monthCol === monthCol2)
        .select(
          tag,
          monthCol,
          round(questionCount / countAllInMonth * lit(100), 2) as 'questions, //<2>
          round(questionWithAnswerCount / questionCount * lit(100), 2) as 'questionWithAnswer, //<3>
          round(questionWithoutAnswerCount / questionCount * lit(100), 2) as 'questionWithoutAnswer, //<4>
          round(questionWithAcceptedAnswerCount / questionCount * lit(100), 2) as 'questionWithAcceptedAnswer //<5>
        )
    }
    //end::inPercentage[]

    def groupByTagAndMonth()(implicit spark: SparkSession): Dataset[TagAndMonthStats] = {
      import spark.implicits._
      stats
        .groupBy(tag, monthCol)
        .agg(
          sum(questionCount) as questionCount,
          sum(answerCount) as answerCount,
          sum(questionWithAnswerCount) as questionWithAnswerCount,
          sum(questionWithoutAnswerCount) as questionWithoutAnswerCount,
          sum(questionWithAcceptedAnswerCount) as questionWithAcceptedAnswerCount,
          sum(viewCount) as viewCount
        )
        .as[TagAndMonthStats]
    }

    //tag::fillMonthGaps[]
    def fillMonthGaps()(implicit spark: SparkSession): Dataset[TagAndMonthStats] = {
      import spark.implicits._

      def zeroByTagAndMonth = {
        val rows: Array[Row] = stats.agg(
          min(monthCol) as 'min,
          max(monthCol) as 'max
        ).collect()

        val minMonth: String = rows(0).getString(0)
        val maxMonth: String = rows(0).getString(1)

        import pl.holowko.bigdata.so.util.time._
        val monthRange: List[String] = dateRange(minMonth, maxMonth, 1.month).map(monthStr).toList

        stats
          .select(tag)
          .distinct()
          .flatMap(row => {
            val tag = row.getString(0)
            monthRange.map(TagAndMonthStats(tag, _, 0, 0, 0, 0, 0, 0))
          })
      }

      stats
        .union(zeroByTagAndMonth)
        .groupByTagAndMonth()
    }
    //end::fillMonthGaps[]
    
    //tag::writeToOneCsv[]
    def writeToOneCsv(path:String): Unit = {
      stats
      .coalesce(1)
      .write
      .option("header", value = true)
      .csv(path)
    }
    //end::writeToOneCsv[]
    
  }
    
  implicit class TagStatsDataset(stats:Dataset[TagStats]) {

      def inPercentage()(implicit spark:SparkSession): DataFrame = {
        import spark.implicits._
        val row = stats.agg(
            sum(questionCount) as questionCount,
            sum(viewCount) as viewCount
          ).first()
        
        val countAll = row.getLong(0)
        val viewCountAll = row.getLong(1)
        
        stats
          .select(
            tag,
            //procent wszystkich pytan
            round(questionCount / countAll * lit(100), 2) as 'questions,
            //procent pytan z odpowiedziami dla danego tagu
            round(questionWithAnswerCount / questionCount * lit(100), 2) as 'questionWithAnswer,
            //procent pytan bez odpowiedzi dla danego tagu
            round(questionWithoutAnswerCount / questionCount * lit(100), 2) as 'questionWithoutAnswer,
            //procent pytan z zaakceptowaną odpowiedzią dla danego tagu
            round(questionWithAcceptedAnswerCount / questionCount * lit(100), 2) as 'questionWithAcceptedAnswer,
            round(viewCount / viewCountAll * lit(100), 2) as 'viewCount
          )
      }

      def writeToOneCsv(path:String): Unit = {
        stats
        .coalesce(1)
        .write
        .option("header", value = true)
        .csv(path)
      }
  }
  
  implicit class TagTimeDataset(tagTime:Dataset[TagTime]) {
    
    def avgMinByTag()(implicit spark:SparkSession): DataFrame = {
      import spark.implicits._
      tagTime
        .groupBy(tag)
        .agg(
          avg(timeMs) as avgTimeToAnswerMs,
          min(timeMs) as minTimeToAnswerMs
        )
        .withColumn(avgTimeToAnswerHuman.name, humanMs(avgTimeToAnswerMs))
        .withColumn(minTimeToAnswerHuman.name, humanMs(minTimeToAnswerMs))
        .sort(avgTimeToAnswerMs.asc)
    }
    
    def average()(implicit spark:SparkSession): DataFrame = {
      import spark.implicits._
      tagTime.agg(
        avg(timeMs) as avgTimeToAnswerMs
      )
      .withColumn(avgTimeToAnswerHuman.name, humanMs(avgTimeToAnswerMs))
    }
  }
  
  implicit class TagUserAnswerCountDataset(stats:Dataset[TagUserAnswerCount]) {

    //tag::userCountByTag[]
    def userCountByTag()(implicit spark:SparkSession): DataFrame = {
      import spark.implicits._
      stats
        .groupBy(tag) //<1>
        .agg(
          count(aOwnerUserId) as countAgg //<2>
        )
        .orderBy(countAgg.desc) //<3>
    }
    //end::userCountByTag[]
      
    def writeToOneCsv(path:String): Unit = {
      stats
        .coalesce(1)
        .write
        .option("header", value = true)
        .csv(path)
    }
    
  }
  
  implicit class DataFrameFun(stats:DataFrame) {

      def writeToOneCsv(path:String): Unit = {
        stats
        .coalesce(1)
        .write
        .option("header", value = true)
        .csv(path)
      }
  }
  
}
