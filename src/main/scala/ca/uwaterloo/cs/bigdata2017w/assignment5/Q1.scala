package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  verify()
}

object Q1 {
	val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new Conf(argv)

		log.info("Input: " + args.input())
		log.info("Date: " + args.date())

  		val conf = new SparkConf().setAppName("Q1")
  		val sc = new SparkContext(conf)

  		val date = args.date()
  		val textFile = sc.textFile(args.input() + "/lineitem.tbl")
  		val count = textFile
  			.map(line => line.split("\\|")(10))
  			.filter(_.contains(date))
  			.count

  		println("ANSWER=" + count)
	}
}
