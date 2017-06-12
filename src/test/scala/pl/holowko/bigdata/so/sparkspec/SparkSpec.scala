package pl.holowko.bigdata.so.sparkspec

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkSpec extends BeforeAndAfterAll { 
  this: Suite =>

  @transient private var _spark: SparkSession = _
  
  implicit def spark:SparkSession = _spark
  
  override def beforeAll(): Unit = {
    if (_spark == null) {
      _spark = SparkSession.builder()
        .appName("SparkSpecTest")
        .master("local[*]")
        .getOrCreate()
      _spark.sparkContext.setLogLevel("ERROR")
    }
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    if (_spark != null) {
      _spark.stop()
      System.clearProperty("spark.driver.port")
      _spark = null
    }
  }
  
}
