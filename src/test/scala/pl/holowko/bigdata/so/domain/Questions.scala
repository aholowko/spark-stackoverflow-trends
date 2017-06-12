package pl.holowko.bigdata.so.domain

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.{Dataset, SparkSession}
import pl.holowko.bigdata.so.util.time._

object Questions {
  def builder(spark:SparkSession) = new QuestionBuilder(spark) 
}

class QuestionBuilder(val spark:SparkSession) {

  private var _qid: Long = 1
  private var _aid: Long = 1
  private var _answerCount: Long = 0
  private var _acceptedAnswerId: Option[Long] = None
  private var _viewCount: Long = 0
  private var _ownerUserId: Option[Long] = None
  private var _tags: Seq[String] = Seq()
  private var _month: String = "197001"
  private var _timestamp: Long = 0
  
  private val _questions = List.newBuilder[Question]
  private val _answers = List.newBuilder[Answer]

  def answerCount(answerCount:Long) : QuestionBuilder = { this._answerCount = answerCount; this }
  def acceptedAnswerId(id:Long): QuestionBuilder = { this._acceptedAnswerId = Some(id); this}
  def viewCount(viewCount:Long): QuestionBuilder = { this._viewCount = viewCount; this}
  def ownerUserId(ownerUserId:Long): QuestionBuilder = { this._ownerUserId = Some(ownerUserId); this}
  def tag(tag:String): QuestionBuilder = { this._tags = Seq(tag); this}
  def tags(tags:Seq[String]): QuestionBuilder = { this._tags = tags; this}
  def month(month:String): QuestionBuilder = { this._month = month; this}
  
  def time(time:DateTime): QuestionBuilder = {
    this._timestamp = time.getMillis
    this._month = time.toString(PATTERN)
    this
  }

  def withAnswer(userId:Long) : QuestionBuilder = {
    _answers += buildOneAnswer(userId)
    _answerCount += 1
    this
  }
  
  def withAnswerAndNullUserId() :QuestionBuilder = {
    _answers += buildOneAnswerWithNoneUserId()
    _answerCount += 1
    this
  }

  def withAnswer(time:DateTime) : QuestionBuilder = {
    _answers += buildOneAnswer(time)
    _answerCount += 1
    this
  }

  def withAcceptedAnswer(time:DateTime) : QuestionBuilder = {
    val answer = buildOneAnswer(time)
    _answers += answer
    _answerCount += 1
    _acceptedAnswerId = Some(answer.aid)
    this
  }

  def withAcceptedAnswer(userId:Long) : QuestionBuilder = {
    val answer = buildOneAnswer(userId)
    _answers += answer
    _answerCount += 1
    _acceptedAnswerId = Some(answer.aid)
    this
  }
  
  def and(): QuestionBuilder = {
    _questions ++= buildOneQuestion()
    this
  }

  private def buildQuestions(): List[Question] = _questions.result()
  
  private def buildAnswers(): List[Answer] = _answers.result()
  
  def ds():Dataset[Question] = {
    import spark.implicits._
    buildQuestions().toDS()
  }
  
  def questionsDS():Dataset[Question] = ds()
  
  def answersDS():Dataset[Answer] = {
    import spark.implicits._
    buildAnswers().toDS()
  }
  
  private def buildOneAnswer(time:DateTime):Answer = {
    val month = time.toString(PATTERN)
    val timestamp = time.getMillis
    _aid += 1
    Answer(_aid, _qid, month, timestamp, _ownerUserId)
  }
  
  private def buildOneAnswer(userId:Long):Answer = {
    _aid += 1
    Answer(_aid, _qid, _month, _timestamp, Some(userId))
  }
  
  private def buildOneAnswerWithNoneUserId():Answer = {
    _aid += 1
    Answer(_aid, _qid, _month, _timestamp, None)
  }

  private def buildOneQuestion():Seq[Question] = {
    val questions:Seq[Question] = if (_tags.isEmpty) {
        Seq(Question(_qid, _answerCount, _acceptedAnswerId, _viewCount, Question.NO_TAG, _month, _timestamp, _ownerUserId))
    } else {
      _tags.map { tag =>
        Question(_qid, _answerCount, _acceptedAnswerId, _viewCount, tag, _month, _timestamp, _ownerUserId)
      }
    }
    clear()
    questions
  }
  
  private def clear(): Unit = {
    _qid += 1
    _answerCount = 0
    _acceptedAnswerId = None
    _ownerUserId = None
    _viewCount = 0
    _tags = Seq()
    _month = "197001"
  }
  
}
