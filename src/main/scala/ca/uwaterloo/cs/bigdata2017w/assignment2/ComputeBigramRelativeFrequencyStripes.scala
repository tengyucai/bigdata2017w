package ca.uwaterloo.cs.bigdata2017w.assignment2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Compute Bigram Relative Frequency Stripes")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) {
          tokens.sliding(2).map(p => {
            (p.head, Map(p.last -> 1.0))
          })
        } else List()
      })
      .reduceByKey((stripe1, stripe2) => {
        stripe1 ++ stripe2.map{ case (k, v) => k -> (v + stripe1.getOrElse(k, 0.0)) }
      })
      .map(stripe => {
        val sum = stripe._2.foldLeft(0.0)(_+_._2)
        (stripe._1, stripe._2 map {case (k, v) => (k, v / sum)})
      })
      .saveAsTextFile(args.output())
  }
}
