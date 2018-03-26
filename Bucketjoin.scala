package joinExperiments

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Bucketjoin extends Serializable {

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

    process(args,true)
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

  def buildSession(masterUrl: String, appName: String) = {
    val conf = new SparkConf()
    conf.set("spark.dynamicAllocation.enabled", "true")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // Stop creating summary metadata after parquet file is written
    conf.set("parquet.enable.summary-metadata", "false")

    val spark =
      SparkSession.builder().master(masterUrl).appName(appName).config(conf).getOrCreate()

    // Use better appending output writer for parquet file to S3 algorithm
    // https://docs.databricks.com/spark/latest/faq/append-slow-with-spark-2.0.0.html
    spark.sparkContext.hadoopConfiguration
      .set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    spark
  }

  def process(arguments: Array[String],shouldBucket: Boolean) = {
    val spark: SparkSession = buildSession("local[*]", "Bucketing test")
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
