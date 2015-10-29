package de.dewarim.learning.spark

import org.apache.spark.SparkContext._ // unused in first version, required for more complex rdds.
import org.apache.spark.{SparkConf, SparkContext}


/* Invocation example: 
  spark-submit --class de.dewarim.learning.spark.SparkMapApp --master local \ 
   target/learning-hadoop-jar-with-dependencies.jar input/logs/fantasy_connector_de.log output/fcd7
 */
object SparkMapApp {
  // Implements the map-reduce job from LogMapper + UrlHitsReducer as a spark-submit program 
  // (see spark/SparkMapper.scala for a version with more comments 
  def hitMapper(line: String): (String, Int) = {
    val fields: Array[String] = line.split("\\s+")
    if (fields.length < 8) {
      return ("invalid input", 1)
    }

    val hostname: String = fields(8).replace("\"", "")
    val file: String = fields(5)
    return (hostname + file, 1)
  }

  def main(args: Array[String]) {
    if (args.length != 2) {
      println("Usage: SparMapApp inputFile outputFile")
      return
    }
    val inputFile = args(0)
    val outputFile = args(1)
    val sparkConf = new SparkConf().setAppName("Spark Log Mapper")
    val sc = new SparkContext(sparkConf)

    var lines = sc.textFile(inputFile)
    // map each line of the rdd to a tuple of $url, 1
    var hits = lines.map(hitMapper) // same as lines.map(line => hitMapper(line))
    var reduced = hits.reduceByKey(_ + _) // same as reduceByKey(add) or reduceByKey((a,b) => a+b)

    // reverse the map so the hits come first. Access the tuple's values with $variableName._$fieldNumber.
    var asString = reduced.map(x => s"${x._2}\t${x._1}")
    asString.saveAsTextFile(outputFile)
    
  }


}