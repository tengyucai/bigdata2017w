package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

object Q4 {
  val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new Conf(argv)

		log.info("Input: " + args.input())
		log.info("Date: " + args.date())

		val conf = new SparkConf().setAppName("Q4")
		val sc = new SparkContext(conf)

    val date = args.date()
		
    val customer = sc.textFile(args.input() + "/customer.tbl")
      .map(line => (line.split("\\|")(0), line.split("\\|")(3).toInt))
    val custBroadcast = sc.broadcast(customer.collectAsMap())

    val nation = sc.textFile(args.input() + "/nation.tbl")
      .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))
    val nationBroadcast = sc.broadcast(nation.collectAsMap())

    val orders = sc.textFile(args.input() + "/orders.tbl")
      .map(line => (line.split("\\|")(0), line.split("\\|")(1)))

    val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
      .filter(line => line.split("\\|")(10).contains(date))
      .map(line => (line.split("\\|")(0), 1))
      .reduceByKey(_ + _)
      .cogroup(orders)
      .filter(_._2._1.size != 0)
      .flatMap(p => {
        var list = scala.collection.mutable.ListBuffer[((Int, String), Int)]()
        val nationKey = custBroadcast.value(p._2._2.head)
        val nationName = nationBroadcast.value(nationKey)
        val counts = p._2._1.iterator
        while (counts.hasNext) {
          list += (((nationKey.toInt, nationName), counts.next()))
        }
        list
      })
      .reduceByKey(_ + _)
      .take(20)
      .foreach(p => println(p._1._1, p._1._2, p._2))
	}
}
