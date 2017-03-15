package ca.uwaterloo.cs.bigdata2017w.assignment6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model, method)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val method = opt[String](descr = "method", required = true)
  verify()
}

object ApplyEnsembleSpamClassifier {
	val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new Conf3(argv)

		log.info("Input: " + args.input())
		log.info("Output: " + args.output())
		log.info("Model: " + args.model())
		log.info("Method: " + args.method())

		val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
		val sc = new SparkContext(conf)

		val outputDir = new Path(args.output())
		FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

		val textFile = sc.textFile(args.input())

		val wX = sc.broadcast(sc.textFile(args.model() + "/part-00000")
			.map(l => {
				val tokens = l.substring(1, l.length()-1).split(",")
				(tokens(0).toInt, tokens(1).toDouble)
			}).collectAsMap())

		val wY = sc.broadcast(sc.textFile(args.model() + "/part-00001")
			.map(l => {
				val tokens = l.substring(1, l.length()-1).split(",")
				(tokens(0).toInt, tokens(1).toDouble)
			}).collectAsMap())

		val wBritney = sc.broadcast(sc.textFile(args.model() + "/part-00002")
			.map(l => {
				val tokens = l.substring(1, l.length()-1).split(",")
				(tokens(0).toInt, tokens(1).toDouble)
			}).collectAsMap())

		def spamminess(features: Array[Int], weights: scala.collection.Map[Int, Double]) : Double = {
			var score = 0d
			features.foreach(f => if (weights.contains(f)) score += weights(f))
			score
		}

		val method = args.method()
		val tested = textFile.map(line => {
			val tokens = line.split(" ")
			val features = tokens.drop(2).map(_.toInt)
			val scoreX = spamminess(features, wX.value)
			val scoreY = spamminess(features, wY.value)
			val scoreBritney = spamminess(features, wBritney.value)
			var score = 0d
			if (method == "average") {
				score = (scoreX + scoreY + scoreBritney) / 3
			} else {
				var voteX = if (scoreX > 0) 1d else -1d
				var voteY = if (scoreY > 0) 1d else -1d
				var voteBritney = if (scoreBritney > 0) 1d else -1d
				score = voteX + voteY + voteBritney
			}
			val classification = if (score > 0) "spam" else "ham"
			(tokens(0), tokens(1), score, classification)
		})

		tested.saveAsTextFile(args.output())
	}
}