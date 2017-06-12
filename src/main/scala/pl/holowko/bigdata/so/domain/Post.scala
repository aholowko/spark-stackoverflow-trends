package pl.holowko.bigdata.so.domain

object Question {
  val NO_TAG = "--no-tag--" 
}

trait Tagged {
  val tag:String
}
//tag::Post[]
sealed abstract class Post(id: Long,                  //<1>
                           month: String,             //<2>
                           timestamp:Long,            //<3>
                           ownerUserId:Option[Long])  //<4>
//end::Post[]

//tag::Question[]
final case class Question(qid: Long,                      //<1>
                          answerCount:Long,               //<2>
                          acceptedAnswerId:Option[Long],  //<3>
                          viewCount:Long,                 //<4>
                          tag:String,                     //<5>
                          month:String,
                          qTimestamp:Long,
                          qOwnerUserId:Option[Long]) 
  extends Post(qid, month, qTimestamp, qOwnerUserId) with Tagged
//end::Question[]

//tag::Answer[]
final case class Answer(aid: Long,                    //<1>
                        parentId: Long,               //<2>
                        month: String,
                        aTimestamp:Long,
                        aOwnerUserId:Option[Long]) 
  extends Post(aid, month, aTimestamp, aOwnerUserId)
//end::Answer[]
