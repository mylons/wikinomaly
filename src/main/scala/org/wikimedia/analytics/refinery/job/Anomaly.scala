package org.wikimedia.analytics.refinery.job

import org.apache.commons.cli.MissingArgumentException
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.surus.math.AugmentedDickeyFuller
import org.surus.math.RPCA

object Anomaly {
  /**
    * Usage: Anomaly [column name] [number of rows] [number of columns] [force diff] [input csv path]
    * based on netflix's example usage https://github.com/Netflix/Surus/blob/master/resources/examples/pig/rad.pig#L55
    * for more insight on how options are set see constructor https://github.com/Netflix/Surus/blob/3f68bd384e2267ca00602febe4a382a55a40eb4d/src/main/java/org/surus/pig/RAD.java#L39-L52
    * and the rest of the code is a literal translation of https://github.com/Netflix/Surus/blob/3f68bd384e2267ca00602febe4a382a55a40eb4d/src/main/java/org/surus/pig/RAD.java#L118-L272
    */
  def main(args: Array[String]): Unit = {

    val colName = if (args.length > 0) args(0) else throw new MissingArgumentException("a column name is required")
    val nRows = if (args.length > 1) args(1).toInt else -1
    val nCols = if (args.length > 2) args(2).toInt else 7
    val csvPath: Option[String] = if (args.length > 3) Some(args(3)) else Option(null)
    val sparkSession = SparkSession.builder.appName("Surus Anomaly").config("spark.master", "local").getOrCreate()
    createRPCADataFrameFromInput(sparkSession, nRows, nCols, colName, csvPath) match {
      case Some(df) =>
        // save df somewhere
        println(df.describe())
      case _ =>
        println("no dataframe returned")
    }
  }

  def createRPCADataFrameFromInput(
                           sparkSession: SparkSession,
                           numRows: Int,
                           numCols: Int,
                           colName: String,
                           csvPath: Option[String],
                           queryTable: String = "",
                           year: Int = 0, month: Int = 0, day: Int = 0, hour: Int = 0
                         ): Option[DataFrame] = {
    val sqlContext = sparkSession.sqlContext

    val df: DataFrame = csvPath match {
      case Some(pathToCsv) =>
        sqlContext.read.format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(pathToCsv) // TODO likely needs to be an s3 path input to the job
          .na.fill(0)
          .sort("date")
      case None =>
        sqlContext.sql(
          // TODO review this query
          s"""
             |SELECT
             | *
             |FROM $queryTable
             |  AND year = $year
             |  AND month = $month
             |  AND day = $day
             |  AND hour = $hour
          """
            .stripMargin
        ).na.fill(0)
          .sort("date") // TODO this will need attention from wiki
    }

    val nRows = if (numRows > 0) numRows else (df.count() / numCols).toInt
    createRPCADataFrameFromDataFrame(sparkSession, df, nRows, numCols, colName)
  }

  def createRPCADataFrameFromDataFrame(sparkSession: SparkSession,
                                       df: DataFrame, numRows: Int, numCols: Int, colName: String): Option[DataFrame] = {
    // other setups that are defaults in the netflix code and don't appear to be configurable
    val sqlContext = sparkSession.sqlContext
    val lpenalty = 1
    val spenalty = lpenalty / Math.sqrt(Math.max(numCols, numRows))
    val minRecords = 2 * numRows
    val numNonZeroRecords = df.filter(s"$colName > 0").count()

    if (numNonZeroRecords >= minRecords) {
      // setup AugmentedDickeyFuller from inputArray
      val inputArray = df.select(colName).collect().map(_.getAs[Int](0).toDouble)
      val dickeyFullerTest = new AugmentedDickeyFuller(inputArray)

      val inputRDD = if (dickeyFullerTest.isNeedsDiff) {
        // Auto Diff
        sparkSession.sparkContext.parallelize(dickeyFullerTest.getZeroPaddedDiff)
      } else {
        sparkSession.sparkContext.parallelize(inputArray)
      }.cache() // TODO this might not be optimal depending on wiki's cluster and input size

      val mean = inputRDD.mean()
      val stdev = inputRDD.stdev()
      // Transformation: Zero Mean, Unit Variance// Transformation: Zero Mean, Unit Variance
      val inputArrayTransformed = inputRDD.map(i => (i - mean) / stdev).collect()

      // this is likely memory intensive on large datasets because it's converting the RDD to an array
      val input2DArray = VectorToMatrix(inputArrayTransformed, numRows, numCols)
      val rSVD = new RPCA(input2DArray, lpenalty, spenalty)
      val outputE = rSVD.getE.getData
      val outputS = rSVD.getS.getData
      val outputL = rSVD.getL.getData

      val newSchema = StructType(df.schema.fields ++ Array(
        StructField("transformed_n", DoubleType, false),
        StructField("L", DoubleType, false),
        StructField("S", DoubleType, false),
        StructField("E", DoubleType, false)))

      def transformRows(iter: Iterator[(Row, Long)]): Iterator[Row] = iter.map(transformRow)

      def transformRow(row: (Row, Long)): Row = {
        val n = row._2.toInt
        val i = n % numRows
        val j = Math.floor(n / numRows).toInt
        Row.fromSeq(row._1.toSeq ++ Array[Any](
          inputArrayTransformed(n),
          outputE(i)(j) * stdev + mean,
          outputS(i)(j) * stdev,
          outputL(i)(j) * stdev))
      }
      return Some(sqlContext.createDataFrame(df.rdd.zipWithIndex().mapPartitions(transformRows), newSchema))
    }
    Option(null)
  }
  // Helper Function
  def VectorToMatrix(x: Array[Double], rows: Int, cols: Int): Array[Array[Double]] = {
    val input2DArray = Array.ofDim[Double](rows, cols)
    for (n <- 0 until x.length) {
      val i = n % rows
      val j = Math.floor(n / rows)
      input2DArray(i)(j.toInt) = x(n)
    }
    input2DArray
  }


}
