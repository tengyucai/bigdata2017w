package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

object Q2 {
	val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new Conf(argv)

		log.info("Input: " + args.input())
		log.info("Date: " + args.date())

  		val conf = new SparkConf().setAppName("Q2")
  		val sc = new SparkContext(conf)

  		val date = args.date()

  		val orders = sc.textFile(args.input() + "/orders.tbl")
  			.map(line => (line.split("\\|")(0), line.split("\\|")(6)))
  		
  		val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
  			.map(line => (line.split("\\|")(0), line.split("\\|")(10)))
  			.filter(_._2.contains(date))
  			.cogroup(orders)
  			.filter(_._2._1.size != 0)
  			.sortByKey()
  			.take(20)
  			.map(p => (p._2._2.head, p._1.toLong))
  			.foreach(println)
	}
}
