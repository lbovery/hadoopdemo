package doppod.sparkSQL

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  *
  * @author lyb 
  */
class sparkSqlTest {

}
object sparkSqlTest {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("WC").setMaster("local[*]"))
    val spark = SparkSession.builder().config(new SparkConf().setAppName("WC").setMaster("local[*]"))
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    spark.sql("show tables").show()
//    val df = spark.read.json("C:\\Users\\ThinkPad\\Downloads\\spark-2.3.3-bin-without-hadoop\\examples/src/main/resources/people.json")
//
//    // Displays the content of the DataFrame to stdout
//    df.show()
//
//    df.filter($"age" > 21).show()
//
//    df.createOrReplaceTempView("persons")
//
//    spark.sql("SELECT * FROM persons where age > 21").show()

    spark.stop()

  }
}
