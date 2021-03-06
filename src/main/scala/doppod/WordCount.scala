package doppod

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author lyb 
  */
class WordCount {

}

object WordCount {
  def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("WC").setMaster("local[*]"))
    //创建SparkConf并设置App名称
    //    val file = "D:\\3.sql"
    //    val tuples: Array[(String, Int)] = sc.textFile(file).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _, 1)
    //      .sortBy(_._2, ascending = false).collect()
    //    tuples.take(5).foreach(println)
    test1(sc)
    //    test2(sc)
  }


  /**
    * 有状态转换
    *
    */
  def test5(sc: SparkContext): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("file:///check")
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(""))

    val pairs = words.map(word => (word, 1))

    val wordCounts = pairs.updateStateByKey((seq: Seq[Int], state: Option[Int]) => Some(state.getOrElse(0) + seq.sum))

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate

  }

  /**
    * 测试spark stream
    */
  def test4(sc: SparkContext): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(""))

    //import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate

  }


  /**
    * combineByKey 方法
    */
  def test1(sc: SparkContext): Unit = {
    val rddTest: RDD[(String, Int)] = sc.makeRDD(Array(("test1", 57), ("test1", 123), ("test2", 34), ("test3", 5), ("test1", 6), ("test2", 54)))
    val tuples = rddTest.combineByKey((_, 1), (c: (Int, Int), v) => (c._1 + v, c._2 + 1), (c1: (Int, Int), c2: (Int, Int)) => (c1._1 + c2._1, c1._2 + c1._2)).collect()
    tuples.foreach(println)
  }

  def test2(sc: SparkContext): Unit = {
    val rddTest: RDD[(String, Int)] = sc.makeRDD(Array(("test1", 57), ("test1", 123), ("test2", 34), ("test3", 5), ("test1", 6), ("test2", 54)))

    val tuples = rddTest.aggregateByKey((0, 0))((c: (Int, Int), v) => (c._1 + v, c._2 + 1), (c1: (Int, Int), c2: (Int, Int)) => (c1._1 + c2._1, c1._2 + c1._2)).collect()
    tuples.foreach(println)
  }

  def test3(sc: SparkContext): Unit = {
    val jdbcUrl = "jdbc:mysql://localhost:3306/sitedust?autoReconnect=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true"
    val rdd = new org.apache.spark.rdd.JdbcRDD(
      sc,
      () => {
        Class.forName("com.mysql.cj.jdbc.Driver").newInstance()
        java.sql.DriverManager.getConnection(jdbcUrl, "root", "@Doppod111111@")
      },
      "select * from station where id >= ? and id <= ?;",
      1,
      100,
      2,
      r => (r.getInt("id"), r.getString("name")))
    println("=====")
    println(rdd.count)
    println("=====")
    rdd.foreach(println)
  }
}
