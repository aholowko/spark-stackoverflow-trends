package pl.holowko.bigdata.so

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import pl.holowko.bigdata.so.agg.{TagAndMonthStats, TagUserAnswerCount}
import pl.holowko.bigdata.so.agg.functions._
import pl.holowko.bigdata.so.domain.columns._
import pl.holowko.bigdata.so.domain.map._
import pl.holowko.bigdata.so.domain.{Answer, Question, Tagged}

import scala.reflect.io.Path
import scala.util.Try

object StackOverflowJob {

  private implicit val spark: SparkSession =
    SparkSession.builder()
      .appName("StackOverflowTrends")
      .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("Wrong parameters. Pass input file path and out dir path")
      return
    }
    val argList:List[String] = args.toList
    val (inFile, outDir, top:Int) = argList match {
      case in :: out :: t :: Nil => (in, out, t.toInt) 
      case in :: out :: Nil => (in, out, 100) 
      case _ => throw new MatchError("Invalid parameters " + argList)
    }

    println(s"inFile: $inFile")
    println(s"outDir: $outDir")
    println(s"top: $top")

    val outPathDir: Path = Path(outDir)

    def inFolder(folder: String) = {
      (outPathDir / folder).path
    }

    println("Trying delete out dir...")
    println(Try(outPathDir.deleteRecursively()))

    val ds: Dataset[String] = spark.read.textFile(inFile)
    val rows: Dataset[String] = ds.filterRows()
    val questions: Dataset[Question] = rows.toQuestions()
    val answers: Dataset[Answer] = rows.toAnswers()

    import spark.implicits._

    printInfo("allStatsByTagAndMonth")
  
    val statsByTagAndMonth: Dataset[TagAndMonthStats] = questions.allStatsByTagAndMonth()
    statsByTagAndMonth.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val allStatsByTagAndMonth = statsByTagAndMonth.fillMonthGaps()
    allStatsByTagAndMonth.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    allStatsByTagAndMonth
      .writeToOneCsv(inFolder("allStatsByTagAndMonth"))

    printInfo("allStatsByTagAndMonthInPercentage")
    allStatsByTagAndMonth
      .inPercentage()
      .writeToOneCsv(inFolder("allStatsByTagAndMonthInPercentage"))

    printInfo("allStatsByTag")
    val allStatsByTag = allStatsByTagAndMonth
      .groupByTag()
      .orderBy(questionCount.desc)
    allStatsByTag.persist(StorageLevel.MEMORY_AND_DISK_SER)
    allStatsByTag
      .writeToOneCsv(inFolder("allStatsByTag"))

    printInfo("allStatsByTagFromTop")
    allStatsByTag
      .limit(top)
      .writeToOneCsv(inFolder("allStatsByTagFromTop"))

    printInfo("allStatsByTagInPercentage")
    allStatsByTag
      .inPercentage()
      .writeToOneCsv(inFolder("allStatsByTagInPercentage"))

    val tagCount: Long = allStatsByTag
      .select(tag).distinct().count()
    println(s"tagCount: $tagCount")

    def topTags(n: Int) = {
      allStatsByTag
        .select(tag)
        .take(n)
        .map(_.getString(0))
    }

    val topNTags: Array[String] = topTags(top)
    val topTagFun: (Tagged) => Boolean = q => topNTags.contains(q.tag)

    printInfo("allStatsByTagAndMonthFromTop")
    val allStatsByTagAndMonthFromTop = allStatsByTagAndMonth
      .filter(topTagFun)
    allStatsByTagAndMonthFromTop.writeToOneCsv(inFolder("allStatsByTagAndMonthFromTop"))

    printInfo("allStatsByTagAndMonthInPercentageFromTop")
    val allStatsByTagAndMonthInPercentageFromTop = allStatsByTagAndMonthFromTop.inPercentage()
    allStatsByTagAndMonthInPercentageFromTop.writeToOneCsv(inFolder("allStatsByTagAndMonthInPercentageFromTop"))

    val timeToFirstAnswer = questions
      .timeToFirstAnswer(answers)
    timeToFirstAnswer.persist(StorageLevel.MEMORY_AND_DISK_SER)

    printInfo("timeToFirstAnswerByTag")
    timeToFirstAnswer
      .avgMinByTag()
      .writeToOneCsv(inFolder("timeToFirstAnswerByTag"))

    printInfo("timeToFirstAnswerAverage")
    timeToFirstAnswer
      .average()
      .writeToOneCsv(inFolder("timeToFirstAnswerAverage"))

    val timeToAcceptedAnswer = questions
      .timeToAcceptedAnswer(answers)
    timeToAcceptedAnswer.persist(StorageLevel.MEMORY_AND_DISK_SER)

    printInfo("timeToAcceptedAnswerByTag")
    timeToAcceptedAnswer
      .avgMinByTag()
      .writeToOneCsv(inFolder("timeToAcceptedAnswerByTag"))

    printInfo("timeToAcceptedAnswerAverage")
    timeToAcceptedAnswer
      .average()
      .writeToOneCsv(inFolder("timeToAcceptedAnswerAverage"))


    
    printInfo("answerCountByTagAndUserId")
    val answerCountByTagAndUserId: Dataset[TagUserAnswerCount] = questions
      .answerCountByTagAndUserId(answers)
    
    answerCountByTagAndUserId
      .writeToOneCsv(inFolder("answerCountByTagAndUserId"))
    
    printInfo("answererCountByTag")
    answerCountByTagAndUserId
      .userCountByTag()
      .writeToOneCsv(inFolder("answererCountByTag"))
    
    printInfo("acceptedAnswerCountByTagUserId")
    val acceptedAnswerCountByTagUserId: Dataset[TagUserAnswerCount] = questions
      .acceptedAnswerCountByTagAndUserId(answers)
    acceptedAnswerCountByTagUserId.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    acceptedAnswerCountByTagUserId
      .writeToOneCsv(inFolder("acceptedAnswerCountByTagUserId"))
    
    printInfo("expertCountByTag")
    acceptedAnswerCountByTagUserId
      .userCountByTag()
      .writeToOneCsv(inFolder("expertCountByTag"))
    
    
    spark.stop()
  }

  private def printInfo(info: String) = {
    println(s"================================== $info ==================================")
  }

}
