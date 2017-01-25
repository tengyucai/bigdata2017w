package ca.uwaterloo.cs.bigdata2017w.assignment2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class PairsPMIConf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(10))
  val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
  val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
  verify()
}

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new PairsPMIConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Pairs PMI")
    val sc = new SparkContext(conf)
    val threshold = args.threshold()

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(), args.reducers())
    val lineNumber = textFile.count()
    val wordCount = textFile
                      .flatMap(line => {
                        val tokens = tokenize(line)
                        if (tokens.length > 0) tokens.take(Math.min(tokens.length, 40)).distinct 
                        else List()
                      })
                      .map(word => (word, 1))
                      .reduceByKey(_ + _)
                      .collectAsMap()
    val broadcastVar = sc.broadcast(wordCount)

    textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        val words = tokens.take(Math.min(tokens.length, 40)).distinct
        if (words.length > 1) {
          var pairs = scala.collection.mutable.ListBuffer[(String, String)]()
          var i = 0
          var j = 0
          for (i <- 0 to words.length - 1) {
            for (j <- 0 to words.length - 1) {
              if ((i != j) && (words(i) != words(j))) {
                var pair : (String, String) = (words(i), words(j))
                pairs += pair
              }
            }
          }
          pairs.toList
        } else List()
      })
      .map(pair => (pair, 1))
      .reduceByKey(_ + _)
      .filter((m) => m._2 >= threshold)
      .map(p => {
        var xCount = broadcastVar.value(p._1._1)
        var yCount = broadcastVar.value(p._1._2)
        var pmi = Math.log10((p._2.toFloat * lineNumber.toFloat) / (xCount * yCount))
        (p._1, (pmi, p._2.toInt))
      })
      .map(p => p._1 + " " + p._2)
      .saveAsTextFile(args.output())
  }
}
