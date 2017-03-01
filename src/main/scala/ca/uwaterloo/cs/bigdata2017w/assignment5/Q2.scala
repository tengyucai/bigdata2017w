package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

object Q2 {
	val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new Conf(argv)

		log.info("Input: " + args.input())
		log.info("Date: " + args.date())

		val conf = new SparkConf().setAppName("Q2")
		val sc = new SparkContext(conf)

		val date = args.date()

    if (args.text()) {
  		val orders = sc.textFile(args.input() + "/orders.tbl")
  			.map(line => (line.split("\\|")(0).toInt, line.split("\\|")(6)))
  		
  		val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
  			.map(line => (line.split("\\|")(0).toInt, line.split("\\|")(10)))
  			.filter(_._2.contains(date))
  			.cogroup(orders)
  			.filter(_._2._1.size != 0)
  			.sortByKey()
  			.take(20)
  			.map(p => (p._2._2.head, p._1.toLong))
  			.foreach(println)
    } else if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate
      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd
      val orders = ordersRDD
        .map(line => (line.getInt(0), line.getString(6)))
      
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val lineitem = lineitemRDD
        .map(line => (line.getInt(0), line.getString(10)))
        .filter(_._2.contains(date))
        .cogroup(orders)
        .filter(_._2._1.size != 0)
        .sortByKey()
        .take(20)
        .map(p => (p._2._2.head, p._1.toLong))
        .foreach(println)
    }
	}
}
