package com.periscope.price.client2.etl.loader

import java.util.concurrent.TimeUnit

import com.periscope.price.client2.etl.builder.SparkSessionBuilder
import com.periscope.price.client2.etl.common.ArgumentChecker
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import scala.util.{Success, Try}

object Bucketjoin extends ArgumentChecker with Serializable {

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    val timeInNanoseconds = t1 - t0
    val timeInSeconds = TimeUnit.SECONDS.convert(timeInNanoseconds, TimeUnit.NANOSECONDS)
    println("Elapsed time: " + timeInNanoseconds + " ns")
    println("Elapsed time: " + timeInSeconds + " seconds")
    result
  }


  def main(args: Array[String]): Unit = {
//    val arguments = args ++ Array("true") // bucket and then join
    val arguments = args // normal join
    val shouldBucket = Try(arguments(0)) match {
      case Success(a) if a != "" => a.toBoolean
      case _ => false
    }

    process(args,shouldBucket)
  }

  def createDataFrames(spark: SparkSession, numberOfDataFrames: Int) = {
    def createTestData(int: Int) = for (i <- List.range(1, 10000000)) yield Row(i, i % int)
    val sqlContext = spark.sqlContext
    val sc = spark.sparkContext
    List.range(1,numberOfDataFrames).map((i: Int) => {
      val schema =
        StructType(
          StructField(s"num1", IntegerType, true) ::
            StructField("col" + i, IntegerType, true) :: Nil)
      sqlContext.createDataFrame(sc.parallelize(createTestData(i), 200), schema)
    })
  }

  def process(arguments: Array[String],shouldBucket: Boolean) = {
    val spark: SparkSession = SparkSessionBuilder.getSession("local[*]", "Bucketing test")
    if(shouldBucket) bucketAndJoin(spark,createDataFrames(spark,4): _*)  else normalJoin(spark,createDataFrames(spark,4): _*)
  }

  def bucketAndJoin(spark: SparkSession, dataFrames: DataFrame*) = {
    time {
      println("------------------------ START: Running with bucketing -----------------------------")
      dataFrames.zipWithIndex.foreach((tuple: (DataFrame, Int)) => {
        val df = tuple._1
        val index = tuple._2
        df.write.bucketBy(10, "num1").saveAsTable("bucketed_large_table_" + index)
      })

      val df = dataFrames.head
      val joinedDFWithBucketing = dataFrames.tail.foldLeft(df)((df1: DataFrame,df2) => df1.join(df2,"num1"))

      joinedDFWithBucketing.printSchema()
      joinedDFWithBucketing.show()
//      println("------------------------ Explain-START: Join Without bucketing -----------------------------")
      //    joinedDFWithBucketing.explain(true)
//      println("------------------------ Explain-END: Join Without bucketing -----------------------------")
      println("------------------------ END: Running with bucketing -----------------------------")

    }
  }

  def normalJoin(spark: SparkSession, dataFrames: DataFrame*) = {
    time {
      println("------------------------ START: Running without bucketing -----------------------------")
      val sqlContext = spark.sqlContext
      val df = dataFrames.head
      val joinedDF = dataFrames.tail.foldLeft(df)((df1: DataFrame,df2) => df1.join(df2,"num1"))

      joinedDF.printSchema()
      joinedDF.show()
//      println("------------------------ Explain-START: Join With bucketing -----------------------------")
//      joinedDF.explain(true)
//      println("------------------------ Explain-END: Join Without bucketing -----------------------------")
      println("------------------------ START: Running without bucketing -----------------------------")

    }
  }

}
