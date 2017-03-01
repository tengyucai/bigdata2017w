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
    val partBroadcast = sc.broadcast(part.collectAsMap())

    val supplier = sc.textFile(args.input() + "/supplier.tbl")
      .map(line => (line.split("\\|")(0), line.split("\\|")(1)))
    val suppBroadcast = sc.broadcast(supplier.collectAsMap())

    val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
      .filter(line => line.split("\\|")(10).contains(date))
      .map(line => {
        val lines = line.split("\\|")
        val orderKey = lines(0)
        val partKey = lines(1)
        val suppKey = lines(2)
        val partTable = partBroadcast.value
        val suppTable = suppBroadcast.value
        (orderKey.toLong, (partTable(partKey), suppTable(suppKey)))
      })
      .sortByKey()
      .take(20)
      .foreach(p => println(p._1, p._2._1, p._2._2))
	}
}
