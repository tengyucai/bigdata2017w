package ca.uwaterloo.cs.bigdata2017w.assignment2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

object StripesPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Stripes PMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
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
      .map(p => {
        var leftCount = broadcastVar.value(p._1._1)
        var rightCount = broadcastVar.value(p._1._2)
        var pmi = Math.log10((p._2.toFloat * lineNumber.toFloat) / (leftCount * rightCount))
        (p._1, (pmi, p._2.toInt))
      })
      .saveAsTextFile(args.output())
  }
}
