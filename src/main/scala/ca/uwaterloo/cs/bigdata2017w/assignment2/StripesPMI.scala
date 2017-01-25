package ca.uwaterloo.cs.bigdata2017w.assignment2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class StripesPMIConf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(10))
  val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
  val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
  verify()
}

object StripesPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new StripesPMIConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Stripes PMI")
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
          var stripes = scala.collection.mutable.ListBuffer[(String, Map[String, Int])]()
          var i = 0
          var j = 0
          for (i <- 0 to words.length - 1) {
            var map = scala.collection.mutable.Map[String, Int]()
            for (j <- 0 to words.length - 1) {
              if ((i != j) && (words(i) != words(j))) {
                var count = map.getOrElse(words(j), 0) + 1
                map(words(j)) = count
              }
            }
            var stripe : (String, Map[String, Int]) = (words(i), map.toMap)
            stripes += stripe
          }
          stripes.toList
        } else List()
      })
      .reduceByKey((stripe1, stripe2) => {
        stripe1 ++ stripe2.map{ case (k, v) => k -> (v + stripe1.getOrElse(k, 0)) }
      })
      .map(stripe => {
        val x = stripe._1
        // for ((k, v) <- stripe._2) {
        //   var pmi = Math.log10((v.toFloat * lineNumber.toFloat) / (broadcastVar.value(x) * broadcastVar.value(k)))
        //   log.info(pmi)
        // }
        // val sum = stripe._2.foldLeft(0.0)(_+_._2)
        // stripe._2.map { case (k, v) => {
        //   k -> Math.log10((v.toFloat * lineNumber.toFloat) / (broadcastVar.value(x) * broadcastVar.value(k)))
        //   // log.info(pmi)
        // }
        // var xCount = broadcastVar.value(p._1._1)
        // var yCount = broadcastVar.value(p._1._2)
        // var pmi = Math.log10((p._2.toFloat * lineNumber.toFloat) / (leftCount * rightCount))
        // (p._1, (pmi, p._2.toInt))
        (stripe._1, stripe._2.filter((m) => m._2 >= threshold).map { case (k, v) => {
          k + "=(" + Math.log10((v.toFloat * lineNumber.toFloat) / (broadcastVar.value(x) * broadcastVar.value(k))) + "," + v + ")"
        }})
      })
      .filter((p) => p._2.size > 0)
      .map(p => p._1 + " {" + (p._2 mkString ", ") + "}")
      .saveAsTextFile(args.output())
  }
}
