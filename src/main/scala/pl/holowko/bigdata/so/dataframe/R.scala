package pl.holowko.bigdata.so.dataframe

import org.apache.spark.sql.Row

class R private(val args:Array[Any] = Array[Any]()) {
  def |(arg:Any) : R = new R(args :+ arg)
  def || : Row = Row(args:_*)
}

object R {
  def ||(arg:Any) : R = new R(Array(arg))
}