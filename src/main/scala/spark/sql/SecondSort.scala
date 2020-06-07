package spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Person(name:String,score:Int)

object SecondSort {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("SecondSort").getOrCreate()
    import spark.implicits._

    val rdd: RDD[String] = spark.sparkContext.textFile("./data/sort/input/sc")
    val personDs: RDD[Person] = rdd.map(one => {
      val arr: Array[String] = one.split(" ")
      Person(arr(0), Integer.valueOf(arr(1)))
    })
    val frame: DataFrame = personDs.toDF()
    frame.createOrReplaceTempView("people")
    spark.sql("select * from people sort by name asc,score desc").show()
  }
}

