package pl.holowko.bigdata.so.agg

import pl.holowko.bigdata.so.domain.Tagged

case class TagTime(
  tag: String,
  timeMs: Long
) extends Tagged

case class TagUserAnswerCount(
  tag: String,
  aOwnerUserId: Long,
  answerCount: Long
) extends Tagged

case class TagStats(
  tag:String,
  questionCount: Long,
  answerCount: Long,
  questionWithAnswerCount: Long,
  questionWithoutAnswerCount: Long,
  questionWithAcceptedAnswerCount: Long,
  viewCount: Long
) extends Tagged

case class TagAndMonthStats(
  tag:String,
  month:String,
  questionCount: Long,
  answerCount: Long,
  questionWithAnswerCount: Long,
  questionWithoutAnswerCount: Long,
  questionWithAcceptedAnswerCount: Long,
  viewCount: Long
) extends Tagged
