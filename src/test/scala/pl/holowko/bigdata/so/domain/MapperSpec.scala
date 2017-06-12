package pl.holowko.bigdata.so.domain

import com.github.nscala_time.time.Imports._
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class MapperSpec extends FlatSpec with GivenWhenThen with Matchers {
  
  "Invalid row" should "map to none" in {
    Given("row with unknown type")
    val line =
      """
        |<row Id="5" PostTypeId="11" CreationDate="2014-05-13T23:58:30.457" />
      """.stripMargin
    
    When("mapping line to question")
    val posts = Mapper.lineToQuestions(line)
    
    Then("Post should be empty")
    posts should be (empty)
  }

  "Invalid line" should "map to none" in {
    Given("invalid line")
    val line =
      """
        |fasfsadffsaf
      """.stripMargin
    
    When("mapping to question")
    val posts = Mapper.lineToQuestions(line)
    
    Then("Post should be empty")
    posts should be (empty)
  }
  
  "Valid question row with tags" should "map to 2 questions" in {
    Given("row with question")
    val line =
      """
        |<row Id="5" PostTypeId="1" CreationDate="2014-05-13T23:58:30.457" AcceptedAnswerId="22" Score="7" ViewCount="338" Tags="&lt;data-mining&gt;&lt;clustering&gt;" Body="dasdas" OwnerUserId="5" LastActivityDate="2014-05-14T00:36:31.077" Title="How can I do simple machine learning without hard-coding behavior?" AnswerCount="1" CommentCount="1" FavoriteCount="1" ClosedDate="2014-05-14T14:40:25.950" />
      """.stripMargin
    
    When("mapping line to questions")
    val posts = Mapper.lineToQuestions(line)
    
    val timestamp = DateTime.parse("2014-05-13T23:58:30.457").getMillis
    
    Then("Posts should have 2 questions")
    posts should contain inOrder (
      Question(5, 1, Some(22), 338, "data-mining", "2014-05-01", timestamp, Some(5)), 
      Question(5, 1, Some(22), 338, "clustering", "2014-05-01", timestamp, Some(5))
    ) 
  }
  
  "Valid question row without AcceptedAnswerId" should "map to 2 questions" in {
    Given("row with question")
    val line =
      """
        |<row Id="7" PostTypeId="1" CreationDate="2015-01-13T23:58:30.457" Score="7" ViewCount="3" Tags="&lt;scala&gt;" Body="dasdas" OwnerUserId="4" LastActivityDate="2014-05-14T00:36:31.077" Title="How can I do simple machine learning without hard-coding behavior?" AnswerCount="1" CommentCount="1" FavoriteCount="1" ClosedDate="2014-05-14T14:40:25.950" />
      """.stripMargin
    
    When("mapping line to questions")
    val posts = Mapper.lineToQuestions(line)
    
    val timestamp = DateTime.parse("2015-01-13T23:58:30.457").getMillis
    
    Then("Posts should have 1 questions")
    posts should contain only Question(7, 1, None, 3, "scala", "2015-01-01", timestamp, Some(4)) 
  }
  
  "Valid question row without OwnerUserId" should "map to 2 questions" in {
    Given("row with question")
    val line =
      """
        |<row Id="7" PostTypeId="1" CreationDate="2015-01-13T23:58:30.457" Score="7" ViewCount="3" Tags="&lt;scala&gt;" Body="dasdas" LastActivityDate="2014-05-14T00:36:31.077" Title="How can I do simple machine learning without hard-coding behavior?" AnswerCount="1" CommentCount="1" FavoriteCount="1" ClosedDate="2014-05-14T14:40:25.950" />
      """.stripMargin
    
    When("mapping line to questions")
    val posts = Mapper.lineToQuestions(line)
    
    val timestamp = DateTime.parse("2015-01-13T23:58:30.457").getMillis
    
    
    Then("Posts should have 1 questions")
    posts should contain only Question(7, 1, None, 3, "scala", "2015-01-01", timestamp, None) 
  }
  
  "Valid question row without tag" should "map to question with special tag" in {
    Given("row with question")
    val line =
      """
        |<row Id="5" PostTypeId="1" CreationDate="2014-05-13T23:58:30.457" Score="7" ViewCount="338" Body="dasdas" OwnerUserId="4" LastActivityDate="2014-05-14T00:36:31.077" Title="How can I do simple machine learning without hard-coding behavior?" AnswerCount="1" CommentCount="1" FavoriteCount="1" ClosedDate="2014-05-14T14:40:25.950" />
      """.stripMargin
    
    When("mapping line to questions")
    val posts = Mapper.lineToQuestions(line)
    
    Then("Posts should have 1 question with special tag")
    val timestamp = DateTime.parse("2014-05-13T23:58:30.457").getMillis
    posts should contain only Question(5, 1, None, 338, Question.NO_TAG, "2014-05-01", timestamp, Some(4))
  }
  
  "Valid question row" should "not map to answer" in {
    Given("row with question")
    val line =
      """
        |<row Id="5" PostTypeId="1" CreationDate="2014-05-13T23:58:30.457" Score="7" ViewCount="338" Tags="&lt;data-mining&gt;&lt;clustering&gt;" Body="dasdas" OwnerUserId="5" LastActivityDate="2014-05-14T00:36:31.077" Title="How can I do simple machine learning without hard-coding behavior?" AnswerCount="1" CommentCount="1" FavoriteCount="1" ClosedDate="2014-05-14T14:40:25.950" />
      """.stripMargin
    
    When("mapping line to answer")
    val posts = Mapper.lineToAnswers(line)
    
    Then("Post should be None")
    posts should be (empty)
  }
  
  "Valid answer row" should "map to answer" in {
    Given("row with answer")
    val line =
      """
        |<row Id="33" PostTypeId="2" ParentId="20" CreationDate="2016-03-14T09:34:15.477" Score="4" Body="test answer" OwnerUserId="132" LastActivityDate="2014-05-14T09:34:15.477" CommentCount="0" />
      """.stripMargin
    
    When("mapping line to answer")
    val posts = Mapper.lineToAnswers(line)
    
    Then("Post should be Answer")
    val timestamp = DateTime.parse("2016-03-14T09:34:15.477").getMillis
    posts should contain only Answer(33, 20, "2016-03-01", timestamp, Some(132))
  }
  
  it should "not map to question" in {
    Given("row with answer")
    val line =
      """
        |<row Id="33" PostTypeId="2" ParentId="20" CreationDate="2014-05-14T09:34:15.477" Score="4" Body="test answer" OwnerUserId="132" LastActivityDate="2014-05-14T09:34:15.477" CommentCount="0" />
      """.stripMargin
    
    When("mapping line to answer")
    val posts = Mapper.lineToQuestions(line)
    
    Then("Post should be empty")
    posts should be (empty)
  }
  
  "Row with different post type" should "be omitted" in {
    Given("row with different ")
    val line =
      """
        |<row Id="17" PostTypeId="5" CreationDate="2014-05-14T02:49:14.580" Score="0" Body="dadasd" OwnerUserId="63" LastEditorUserId="63" LastEditDate="2014-05-16T13:44:53.470" LastActivityDate="2014-05-16T13:44:53.470" CommentCount="0" />
      """.stripMargin
    
    When("mapping line to questions")
    val posts = Mapper.lineToQuestions(line)
    
    Then("Post should be empty")
    posts should be (empty)
  }
  
}
