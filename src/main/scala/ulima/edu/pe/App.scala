package ulima.edu.pe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author ${user.name}
 */
object App {

  def getFile() : RDD[String] = {
    return MySparkContext.getSparkContext().textFile("data/FullData.csv")
  }

  def getSelectablePlayersRDD() : RDD[Array[String]] = {
    return getFile().map( x => x.split(",") )
            .filter( fields => fields(14).toInt < 24 )
  }

  def main(args : Array[String]) {
    getGKs()
  }

  def getGKs() {
    val max2 = getGKStream().take(2)
    MySparkContext.getSparkContext().parallelize(max2)
      .map( fields => fields(0) + "," + fields(1) )
      .saveAsTextFile("data/resultadoGK/")
  }

  def getGKStream() : RDD[Array[String]] = {
    getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase == "GK" )
            .filter( fields => fields(0) != "" && fields(14) != "" &&
                               fields(25) != "" && fields(48) != "" &&
                               fields(49) != "" && fields(50) != "" &&
                               fields(51) != "" && fields(52) != "" )
            .map( fields => Array(fields(0), fields(14) + "," + fields(15) + ","
                      + fields(25) + "," + fields(48) + "," + fields(49) + ","
                      + fields(50) + "," + fields(51) + "," + fields(52) ) )
            .map( fields =>  Array(fields(0),
            MyMath.getRddPercentile( fields(1), 50 ).toString ) )
            .sortBy( fields => fields(1).toDouble, ascending = false )
  }

}

object MySparkContext {

  var sc : SparkContext = null

  def getSparkContext() : SparkContext = {
    if(this.sc == null) {
      var conf = new SparkConf().setAppName("bigdata-taller2").setMaster("local")
      this.sc = new SparkContext(conf)
    }
    return this.sc
  }

}

object MyMath {

  def getRddPercentile(inputScore: String, percentile: Double): Double = {
    val entryArray = inputScore.split(",")
    val numEntries = entryArray.length.toDouble
    val retrievedEntry = (percentile * numEntries / 100.0 ).min(numEntries).max(0).toInt

    return entryArray
            .flatMap( scoreArray => for (s <- scoreArray) yield s )
            .map( score => score.toDouble )
            .sortBy { case (score) => score }
            .zipWithIndex
            .filter { case (score, index) => index == retrievedEntry }
            .map { case (score, index) => score }
            .take(1)(0)
  }

}
