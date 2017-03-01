package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

object Q7 {
  val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new Conf(argv)

		log.info("Input: " + args.input())
		log.info("Date: " + args.date())

		val conf = new SparkConf().setAppName("Q7")
		val sc = new SparkContext(conf)

    val date = args.date()
		
    if (args.text()) {
      val customer = sc.textFile(args.input() + "/customer.tbl")
        .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))
      val custBroadcast = sc.broadcast(customer.collectAsMap())

      val orders = sc.textFile(args.input() + "/orders.tbl")
        .filter(line => {
          (line.split("\\|")(4) < date) && (custBroadcast.value.contains(line.split("\\|")(0).toInt))
        })
        .map(line => {
          val fields = line.split("\\|")
          val orderKey = fields(0).toInt
          val custName = custBroadcast.value(fields(1).toInt)
          val orderDate = fields(4)
          val shipPriority = fields(5)
          (orderKey, (custName, orderDate, shipPriority))
        })

      val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
        .filter(line => line.split("\\|")(10) > date)
        .map(line => {
          val fields = line.split("\\|")
          val revenue = fields(5).toDouble * (1 - fields(6).toDouble)
          (fields(0).toInt, revenue)
        })
        .cogroup(orders)
        .filter(p => p._2._1.size != 0 && p._2._2.size != 0)
        .map(p => {
          val custName = p._2._2.head._1
          val orderDate = p._2._2.head._2
          val shipPriority = p._2._2.head._3
          val orderKey = p._1
          val revenue = p._2._1.foldLeft(0.0)((a, b) => a + b)
          (revenue, (custName, orderKey, orderDate, shipPriority))
        })
        .sortByKey(false)
        .collect()
        .take(10)
        .foreach(p => println(p._2._1, p._2._2, p._1, p._2._3, p._2._4))
    } else if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate
      val customerDF = sparkSession.read.parquet("TPC-H-0.1-PARQUET/customer")
      val customerRDD = customerDF.rdd
      val customer = customerRDD
        .map(line => (line.getInt(0), line.getString(1)))
      val custBroadcast = sc.broadcast(customer.collectAsMap())

      val ordersDF = sparkSession.read.parquet("TPC-H-0.1-PARQUET/orders")
      val ordersRDD = ordersDF.rdd
      val orders = ordersRDD
        .filter(line => {
          (line.getString(4) < date) && (custBroadcast.value.contains(line.getInt(0)))
        })
        .map(line => {
          val orderKey = line.getInt(0)
          val custName = custBroadcast.value(line.getInt(1))
          val orderDate = line.getString(4)
          val shipPriority = line.getString(5)
          (orderKey, (custName, orderDate, shipPriority))
        })

      val lineitemDF = sparkSession.read.parquet("TPC-H-0.1-PARQUET/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val lineitem = lineitemRDD
        .filter(line => line.getString(10) > date)
        .map(line => {
          val revenue = line.getDouble(5) * (1 - line.getDouble(6))
          (line.getInt(0), revenue)
        })
        .cogroup(orders)
        .filter(p => p._2._1.size != 0 && p._2._2.size != 0)
        .map(p => {
          val custName = p._2._2.head._1
          val orderDate = p._2._2.head._2
          val shipPriority = p._2._2.head._3
          val orderKey = p._1
          val revenue = p._2._1.foldLeft(0.0)((a, b) => a + b)
          (revenue, (custName, orderKey, orderDate, shipPriority))
        })
        .sortByKey(false)
        .collect()
        .take(10)
        .foreach(p => println(p._2._1, p._2._2, p._1, p._2._3, p._2._4))
    }
	}
}
