package stats

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Analyzer extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR") 

  println("")
  println("******************************************************")

  // To have the args, the command to run is sbt "run --data ./data/ml-100k/u.data --json ./answers_analyzer.json".
  var conf = new Conf(args) 
  println("Loading data from: " + conf.data()) 
  val dataFile = spark.sparkContext.textFile(conf.data())
  val data = dataFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }) 
  assert(data.count == 100000, "Invalid data")

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

        def avg(groupByArg : String) : (Double, Double, Double, Boolean, Double) = {
          val group = groupByArg match {
            case "user" => data.groupBy(_.user)
            case "item" => data.groupBy(_.item)
          }
          val avgs = group.map(x => (x._1, x._2.map(rat => rat.rating).sum / x._2.size)).collect
          val onlyAvgs = avgs.map(x => x._2)
          val minAvg = onlyAvgs.min
          val maxAvg = onlyAvgs.max
          val avgAvg = onlyAvgs.sum / onlyAvgs.size
          val allUsersClose = ((minAvg - avgAvg > -0.5) && (minAvg - avgAvg < 0.5) && (maxAvg - avgAvg > -0.5) && (maxAvg - avgAvg < 0.5))
          val closeAvgs = onlyAvgs.filter(avg => (avg - avgAvg > -0.5) && (avg - avgAvg < 0.5))
          val ratioClose = closeAvgs.size.toDouble / onlyAvgs.size

          return (minAvg, maxAvg, avgAvg, allUsersClose, ratioClose)
        }

        // Q3.1.2
        val userAvg = avg("user")

        // Q3.1.3
        val itemAvg = avg("item")


        val answers: Map[String, Any] = Map(
          "Q3.1.1" -> Map(
            "GlobalAverageRating" -> data.map(_.rating).sum / data.count // Datatype of answer: Double
          ),
          "Q3.1.2" -> Map(
            "UsersAverageRating" -> Map(
                // Using as your input data the average rating for each user,
                // report the min, max and average of the input data.
                "min" -> userAvg._1,  // Datatype of answer: Double
                "max" -> userAvg._2, // Datatype of answer: Double
                "average" -> userAvg._3 // Datatype of answer: Double
            ),
            "AllUsersCloseToGlobalAverageRating" -> userAvg._4, // Datatype of answer: Boolean
            "RatioUsersCloseToGlobalAverageRating" -> userAvg._5 // Datatype of answer: Double
          ),
          "Q3.1.3" -> Map(
            "ItemsAverageRating" -> Map(
                // Using as your input data the average rating for each item,
                // report the min, max and average of the input data.
                "min" -> itemAvg._1,  // Datatype of answer: Double
                "max" -> itemAvg._2, // Datatype of answer: Double
                "average" -> itemAvg._3 // Datatype of answer: Double
            ),
            "AllItemsCloseToGlobalAverageRating" -> itemAvg._4, // Datatype of answer: Boolean
            "RatioItemsCloseToGlobalAverageRating" -> itemAvg._5 // Datatype of answer: Double
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