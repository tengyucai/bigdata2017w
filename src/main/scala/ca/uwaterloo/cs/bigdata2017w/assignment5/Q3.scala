package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

object Q3 {
  val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new Conf(argv)

		log.info("Input: " + args.input())
		log.info("Date: " + args.date())

  		val conf = new SparkConf().setAppName("Q3")
  		val sc = new SparkContext(conf)

      val date = args.date()
  		
      val part = sc.textFile(args.input() + "/part.tbl")
        .map(line => (line.split("\\|")(0), line.split("\\|")(1)))

      val supplier = sc.textFile(args.input() + "/supplier.tbl")
        .map(line => (line.split("\\|")(0), line.split("\\|")(1)))

      val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
        .map(line => (line.split("\\|")(0), line.split("\\|")(10)))
	}
}
