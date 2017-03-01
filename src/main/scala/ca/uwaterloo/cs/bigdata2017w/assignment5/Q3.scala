package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

object Q3 {
  val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new Conf(argv)

		log.info("Input: " + args.input())
		log.info("Date: " + args.date())

		val conf = new SparkConf().setAppName("Q3")
		val sc = new SparkContext(conf)

    val date = args.date()
		
    if (args.text()) {
      val part = sc.textFile(args.input() + "/part.tbl")
        .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))
      val partBroadcast = sc.broadcast(part.collectAsMap())

      val supplier = sc.textFile(args.input() + "/supplier.tbl")
        .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))
      val suppBroadcast = sc.broadcast(supplier.collectAsMap())

      val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
        .filter(line => line.split("\\|")(10).contains(date))
        .map(line => {
          val lines = line.split("\\|")
          val orderKey = lines(0).toInt
          val partKey = lines(1).toInt
          val suppKey = lines(2).toInt
          val partTable = partBroadcast.value
          val suppTable = suppBroadcast.value
          (orderKey, (partTable(partKey), suppTable(suppKey)))
        })
        .sortByKey()
        .take(20)
        .foreach(p => println(p._1, p._2._1, p._2._2))
    } else if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate
      val partDF = sparkSession.read.parquet("TPC-H-0.1-PARQUET/part")
      val partRDD = partDF.rdd
      val part = partRDD
        .map(line => (line.getInt(0), line.getString(1)))
      val partBroadcast = sc.broadcast(part.collectAsMap())

      val supplierDF = sparkSession.read.parquet("TPC-H-0.1-PARQUET/supplier")
      val supplierRDD = supplierDF.rdd
      val supplier = supplierRDD
        .map(line => (line.getInt(0), line.getString(1)))
      val suppBroadcast = sc.broadcast(supplier.collectAsMap())

      val lineitemDF = sparkSession.read.parquet("TPC-H-0.1-PARQUET/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val lineitem = lineitemRDD
        .filter(line => line.getString(10).contains(date))
        .map(line => {
          val orderKey = line.getInt(0)
          val partKey = line.getInt(1)
          val suppKey = line.getInt(2)
          val partTable = partBroadcast.value
          val suppTable = suppBroadcast.value
          (orderKey, (partTable(partKey), suppTable(suppKey)))
        })
        .sortByKey()
        .take(20)
        .foreach(p => println(p._1, p._2._1, p._2._2))
    }
	}
}
