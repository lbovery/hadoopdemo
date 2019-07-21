package doppod.streaming

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author lyb 
  */
class SparkStreamTest {

}

object SparkStreamTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")


  }
}
