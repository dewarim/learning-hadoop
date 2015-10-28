// Implements the map-reduce job from LogMapper + UrlHitsReducer as a spark-shell script
def hitMapper(line: String):(String, Int) = {
  val fields: Array[String] = line.split("\\s+")
  if (fields.length < 8) {
    return ("invalid input",1)
  }

  val hostname: String = fields(8).replace("\"", "")
  val file: String = fields(5)
  return (hostname + file, 1)
}

// sc is the sparkContext
var lines = sc.textFile("input/logs/fantasy_connector_de.log")
// map each line of the rdd to a tuple of $url, 1
var hits = lines.map(hitMapper) // same as lines.map(line => hitMapper(line))

// reduce the hits to ($url,$count)
/*
 * Can be also written as:
 * var reduced = hits.reduceByKey((a:Int, b:Int) => {
 * println(s"a: $a, b: $b")
 * a + b}
 * ) 
 * - iterate over all values for each key
 * - call the given function for with two values to combine them (and so "reduce" the list of mapping results)
 * 
 * "reduceByKey(), in contrast, accepts a function that turns exactly two values into exactly one — 
 *  here, a simple addition function that maps two numbers to their sum"
 * => see: http://blog.cloudera.com/blog/2014/09/how-to-translate-from-mapreduce-to-apache-spark/
 */

var reduced = hits.reduceByKey(_ + _) // same as reduceByKey(add) or reduceByKey((a,b) => a+b)

// reverse the map so the hits come first. Access the tuple's values with $variableName._$fieldNumber.
var asString = reduced.map(x => s"${x._2}\t${x._1}")
asString.saveAsTextFile("output/fcd4")

