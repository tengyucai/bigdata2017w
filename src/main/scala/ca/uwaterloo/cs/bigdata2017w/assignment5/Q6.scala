package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

object Q6 {
	val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new Conf(argv)

		log.info("Input: " + args.input())
		log.info("Date: " + args.date())

		val conf = new SparkConf().setAppName("Q6")
		val sc = new SparkContext(conf)

		val date = args.date()
		val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
			.filter(line => line.split("\\|")(10).contains(date))
      .map(line => {
        val fields = line.split("\\|")
        val returnFlag = fields(8)
        val lineStatus = fields(9)
        val quantity = fields(4).toLong
        val extendedPrice = fields(5).toDouble
        val discount = fields(6).toDouble
        val tax = fields(7).toDouble
        val discPrice = extendedPrice * (1 - discount)
        val charge = discPrice * (1 - tax)
        ((returnFlag, lineStatus), (quantity, extendedPrice, discPrice, charge, discount, 1))
      })
      .reduceByKey((x, y) => (x._1+y._1, x._2+y._2, x._3+y._3, x._4+y._4, x._5+y._5, x._6+y._6))
      .collect()
      .foreach(p => {
        val count = p._2._6
        println(p._1._1, p._1._2, p._2._1, p._2._2, p._2._3, p._2._4, p._2._1/count, p._2._2/count, p._2._5/count, count)
      })
	}
}
