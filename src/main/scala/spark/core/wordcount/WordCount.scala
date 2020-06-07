package spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCount")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("./data/wordcount/input/wc")
    rdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2,false).foreach(println)
  }
}
