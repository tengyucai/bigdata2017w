package ca.uwaterloo.cs.bigdata2017w.assignment6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "model path", required = true)
  verify()
}

object ApplySpamClassifier {
	val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new Conf2(argv)

		log.info("Input: " + args.input())
		log.info("Output: " + args.output())
		log.info("Model: " + args.model())

		val conf = new SparkConf().setAppName("ApplySpamClassifier")
		val sc = new SparkContext(conf)

		val outputDir = new Path(args.output())
		FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

		val textFile = sc.textFile(args.input())

		val wBroadcast = sc.broadcast(sc.textFile(args.model() + "/part-00000")
			.map(l => {
				val tokens = l.substring(1, l.length()-1).split(",")
				(tokens(0).toInt, tokens(1).toDouble)
			}).collectAsMap())

		def spamminess(features: Array[Int]) : Double = {
			var score = 0d
			features.foreach(f => if (wBroadcast.value.contains(f)) score += wBroadcast.value(f))
			score
		}

		val tested = textFile.map(line => {
			val tokens = line.split(" ")
			val features = tokens.drop(2).map(_.toInt)
			val score = spamminess(features)
			val classification = if (score > 0) "spam" else "ham"
			(tokens(0), tokens(1), score, classification)
		})

		tested.saveAsTextFile(args.output())
	}
}