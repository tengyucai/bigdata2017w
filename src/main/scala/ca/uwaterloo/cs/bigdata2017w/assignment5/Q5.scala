package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

object Q5 {
	val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new Conf(argv)

		log.info("Input: " + args.input())

		val conf = new SparkConf().setAppName("Q5")
		val sc = new SparkContext(conf)

    if (args.text()) {
      val customer = sc.textFile(args.input() + "/customer.tbl")
        .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(3).toInt))
        .filter(p => (p._2 == 3 || p._2 == 24))
      val custBroadcast = sc.broadcast(customer.collectAsMap())

      val nation = sc.textFile(args.input() + "/nation.tbl")
        .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))
      val nationBroadcast = sc.broadcast(nation.collectAsMap())

  		val orders = sc.textFile(args.input() + "/orders.tbl")
  			.map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1).toInt))
  		
  		val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
  			.map(line => {
          val orderKey = line.split("\\|")(0).toInt
          val shipdate = line.split("\\|")(10)
          (orderKey, shipdate.substring(0, shipdate.lastIndexOf('-')))
        })
        .cogroup(orders)
        .filter(_._2._1.size != 0)
        .flatMap(p => {
          var list = scala.collection.mutable.ListBuffer[((String, String), Int)]()
          if (custBroadcast.value.contains(p._2._2.head)) {
            val nationKey = custBroadcast.value(p._2._2.head)
            val nationName = nationBroadcast.value(nationKey)
            val dates = p._2._1.iterator
            while (dates.hasNext) {
              list += (((dates.next(), nationName), 1))
            }
          }
          list
        })
        .reduceByKey(_ + _)
        .sortBy(_._1)
        .collect()
        .foreach(p => println(p._1._1, p._1._2, p._2))
    } else if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate
      val customerDF = sparkSession.read.parquet(args.input() + "/customer")
      val customerRDD = customerDF.rdd
      val customer = customerRDD
        .map(line => (line.getInt(0), line.getInt(3)))
        .filter(p => (p._2 == 3 || p._2 == 24))
      val custBroadcast = sc.broadcast(customer.collectAsMap())

      val nationDF = sparkSession.read.parquet(args.input() + "/nation")
      val nationRDD = nationDF.rdd
      val nation = nationRDD
        .map(line => (line.getInt(0), line.getString(1)))
      val nationBroadcast = sc.broadcast(nation.collectAsMap())

      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd
      val orders = ordersRDD
        .map(line => (line.getInt(0), line.getInt(1)))
      
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val lineitem = lineitemRDD
        .map(line => {
          val orderKey = line.getInt(0)
          val shipdate = line.getString(10)
          (orderKey, shipdate.substring(0, shipdate.lastIndexOf('-')))
        })
        .cogroup(orders)
        .filter(_._2._1.size != 0)
        .flatMap(p => {
          var list = scala.collection.mutable.ListBuffer[((String, String), Int)]()
          if (custBroadcast.value.contains(p._2._2.head)) {
            val nationKey = custBroadcast.value(p._2._2.head)
            val nationName = nationBroadcast.value(nationKey)
            val dates = p._2._1.iterator
            while (dates.hasNext) {
              list += (((dates.next(), nationName), 1))
            }
          }
          list
        })
        .reduceByKey(_ + _)
        .sortBy(_._1)
        .collect()
        .foreach(p => println(p._1._1, p._1._2, p._2))
    }
	}
}
