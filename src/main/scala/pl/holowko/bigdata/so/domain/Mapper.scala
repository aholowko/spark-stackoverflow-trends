package pl.holowko.bigdata.so.domain

import java.text.SimpleDateFormat
import java.time.Instant

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.xml.XML

//tag::map[]
object map {
  
  private val onlyRows: (String) => Boolean = _.trim().startsWith("<row")
  
  implicit class PostMapper(rows:Dataset[String]) {
    
    def filterRows():Dataset[String] = rows.filter(onlyRows) //<1>
    
    def toQuestions()(implicit spark:SparkSession):Dataset[Question] = {
      import spark.implicits._
      rows.flatMap(Mapper.lineToQuestions) //<2>
    }
    
    def toAnswers()(implicit spark:SparkSession):Dataset[Answer] = {
      import spark.implicits._
      rows.flatMap(Mapper.lineToAnswers) //<3>
    }
  }
}
//end::map[]

private object Mapper {

  private val DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"

  def lineToQuestions(line: String): Seq[Question] = {
    lineToPosts(line, 1).collect {
      case q: Question => q
    }
  }

  def lineToAnswers(line: String): Seq[Answer] = {
    lineToPosts(line, 2).collect {
      case q: Answer => q
    }
  }
  
  private def lineToPosts(line: String, onlyPostTypeId:Int): Seq[Post] = {
    try {
      val xml = XML.loadString(line)
      val postTypeId = (xml \ "@PostTypeId").text.toInt
      
      if (onlyPostTypeId != postTypeId) {
        return Seq()
      }
      
      val id = (xml \ "@Id").text.toLong
      val creationDateString = (xml \ "@CreationDate").text
      val sdf = new SimpleDateFormat(DATE_FORMAT)
      val creationDate: Instant = sdf.parse(creationDateString).toInstant

      import pl.holowko.bigdata.so.util.time._

      val month = monthStr(creationDate)
      val timestamp = creationDate.toEpochMilli
      
      val ownerUserId:Option[Long] = (xml \ "@OwnerUserId")
        .find(n => n.nonEmpty)
        .map(n => n.text.toLong)
            
      postTypeId match {
        case 1 => //question
          val viewCount = (xml \ "@ViewCount").text.toInt
          val answerCount = (xml \ "@AnswerCount").text.toLong
          val acceptedAnswerId: Option[Long] = (xml \ "@AcceptedAnswerId")
            .find(n => n.nonEmpty)
            .map(n => n.text.toLong)
          val tagString = (xml \ "@Tags").text
          val splitTags = if (tagString.length == 0) Array[String]() else tagString.substring(1, tagString.length - 1).split("><")
          if (splitTags.isEmpty) {
              Seq(Question(id, answerCount, acceptedAnswerId, viewCount, Question.NO_TAG, month, timestamp, ownerUserId))
          } else {
            splitTags.toList.map { tag =>
              Question(id, answerCount, acceptedAnswerId, viewCount, tag, month, timestamp, ownerUserId)
            }
          }
        case 2 => //answer
          val parentId = (xml \ "@ParentId").text.toLong
          Seq(Answer(id, parentId, month, timestamp, ownerUserId))
        case _ => //no interesting type
          Seq()
      }
    } catch {
      case e: Exception ⇒
        println(s"failed to parse line: $line")
        Seq()
    }

  }
}
