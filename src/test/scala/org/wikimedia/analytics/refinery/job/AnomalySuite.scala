package org.wikimedia.analytics.refinery.job

import org.apache.spark.sql.functions.to_date

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.{SparkConf}
import org.scalatest.FunSuite


class AnomalySuite extends FunSuite {
  val sparkConf = new SparkConf()
    .setAppName("Tests")
    .setMaster("local")
  val sparkSession = SparkSession.builder.appName("AnomalySuite").config("spark.master", "local").getOrCreate()
  val sqlContext = sparkSession.sqlContext

  test("create RPCA from existing dataframe works") {
    import sqlContext.implicits._
    val df = Seq(
      ("fa","article","2014-08-01",104,1.800379224908143,86.80966406407411,17.190335935925887,0.0),
      ("fa","article","2014-08-02",85,-0.18584559740987208,86.74589149148143,-1.7458914914814294,0.0),
      ("fa","article","2014-08-03",88,0.12776884821928816,86.80966406407411,1.1903359359258865,0.0),
      ("fa","article","2014-08-04",96,0.9640740365637155,86.80966406407411,9.190335935925887,0.0),
      ("fa","article","2014-08-05",80,-0.7085363401251392,86.74589149148143,-6.74589149148143,0.0),
      ("fa","article","2014-08-06",85,-0.18584559740987208,86.74589149148143,-1.7458914914814294,0.0),
      ("fa","article","2014-08-07",69,-1.8584559740987268,86.74589149148143,-17.74589149148143,0.0),
      ("fa","article","2014-08-08",81,-0.6039981915820858,86.74589149148143,-5.74589149148143,0.0),
      ("fa","article","2014-08-09",93,0.6504595909345553,86.80966406407411,6.190335935925887,0.0 ))
      .toDF("language", "article", "tmpdate", "num_requests", "transformed_n", "L", "S", "E")
    val testDF = df.withColumn("date", to_date(df.col("tmpdate"))).drop("tmpdate")
    val resultDF = Anomaly.createRPCADataFrameFromDataFrame(sparkSession, testDF.drop("transformed_n", "L", "S", "E"), 4, 9, "num_requests").get
    val t1 = testDF.select( "language", "article", "date", "num_requests", "transformed_n", "L", "S", "E").cache()
    val t2 = resultDF.select( "language", "article", "date", "num_requests", "transformed_n", "L", "S", "E").cache()
    assert(t1.except(t2).count == 0)
    assert(t2.except(t1).count == 0)

  }
}
