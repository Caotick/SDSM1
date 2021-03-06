package predict

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Predictor extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR") 

  println("")
  println("******************************************************")

  // To have the right args, the command to run is sbt "run --train ./data/ml-100k/u1.base --test ./data/ml-100k/u1.test --json ./answers_predictor.json"
  var conf = new Conf(args) 
  println("Loading training data from: " + conf.train()) 
  val trainFile = spark.sparkContext.textFile(conf.train())
  val train = trainFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }) 
  assert(train.count == 80000, "Invalid training data")

  println("Loading test data from: " + conf.test()) 
  val testFile = spark.sparkContext.textFile(conf.test())
  val test = testFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }) 
  assert(test.count == 20000, "Invalid test data")

  val globalPred = 3.0
  val globalMae = test.map(r => scala.math.abs(r.rating - globalPred)).reduce(_+_) / test.count.toDouble

  // Save answers as JSON
  def printToFile(content: String, 
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }
  conf.json.toOption match {
    case None => ; 
    case Some(jsonFile) => {
      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats

        def scale(x: Double, userAvg: Double): Double = {
          if (x > userAvg) return 5 - userAvg 
          else if (x < userAvg) return userAvg - 1
          else return 1
        } 

        val groupUser = train.groupBy(_.user)
        val userAvg = groupUser.map(x => (x._1, x._2.map(rat => rat.rating).sum / x._2.size)).collect
        println(userAvg.getClass)

        val normDevs = train.map(x => (x.rating - 1))

        val answers: Map[String, Any] = Map(
            "Q3.1.4" -> Map(
              "MaeGlobalMethod" -> 0.0, // Datatype of answer: Double
              "MaePerUserMethod" -> 0.0, // Datatype of answer: Double
              "MaePerItemMethod" -> 0.0, // Datatype of answer: Double
              "MaeBaselineMethod" -> 0.0 // Datatype of answer: Double
            ),

            "Q3.1.5" -> Map(
              "DurationInMicrosecForGlobalMethod" -> Map(
                "min" -> 0.0,  // Datatype of answer: Double
                "max" -> 0.0,  // Datatype of answer: Double
                "average" -> 0.0 // Datatype of answer: Double
              ),
              "DurationInMicrosecForPerUserMethod" -> Map(
                "min" -> 0.0,  // Datatype of answer: Double
                "max" -> 0.0,  // Datatype of answer: Double
                "average" -> 0.0 // Datatype of answer: Double
              ),
              "DurationInMicrosecForPerItemMethod" -> Map(
                "min" -> 0.0,  // Datatype of answer: Double
                "max" -> 0.0,  // Datatype of answer: Double
                "average" -> 0.0 // Datatype of answer: Double
              ),
              "DurationInMicrosecForBaselineMethod" -> Map(
                "min" -> 0.0,  // Datatype of answer: Double
                "max" -> 0.0, // Datatype of answer: Double
                "average" -> 0.0 // Datatype of answer: Double
              ),
              "RatioBetweenBaselineMethodAndGlobalMethod" -> 0.0 // Datatype of answer: Double
            ),
         )
        json = Serialization.writePretty(answers)
      }

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}
