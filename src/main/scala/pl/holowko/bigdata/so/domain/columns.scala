package pl.holowko.bigdata.so.domain

object columns {

  val qid = 'qid
  val aid = 'aid
  val tag = 'tag
  val viewCount = 'viewCount
  val answerCount = 'answerCount
  val monthCol = 'month
  val monthCol2 = 'month2
  val acceptedAnswerId = 'acceptedAnswerId
  val qTimestamp = 'qTimestamp
  val aTimestamp = 'aTimestamp
  val parentId = 'parentId
  val aOwnerUserId = 'aOwnerUserId
  val qOwnerUserId = 'qOwnerUserId
  
  val countAgg = 'countAgg
  val countAll = 'countAll
  val countAllInMonth = 'countAllInMonth
  val questionCount = 'questionCount
  val acceptedAnswerCount = 'acceptedAnswerCount
  val avgAnswerCount = 'avgAnswerCount
  val questionWithAnswerCount = 'questionWithAnswerCount
  val questionWithoutAnswerCount = 'questionWithoutAnswerCount
  val questionWithAcceptedAnswerCount = 'questionWithAcceptedAnswerCount
  
  val timeMs = 'timeMs
  val timeToAnswerMs = 'timeToAnswerMs
  val avgTimeToAnswerMs = 'avgTimeToAnswerMs
  val minTimeToAnswerMs = 'minTimeToAnswerMs
  val avgTimeToAnswerHuman = 'avgTimeToAnswerHuman
  val minTimeToAnswerHuman = 'minTimeToAnswerHuman
}
