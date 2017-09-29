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

  def getGKs() {
    val max2 = GK.getGKStream().take(2)
    MySparkContext.getSparkContext().parallelize(max2)
      .map( fields => fields(0) + "," + fields(1) )
      .saveAsTextFile("data/resultadoGK/")
  }

  def main(args : Array[String]) {
    getGKs()
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

  def getRddPercentile(inputScore: RDD[String], percentile: Double): Double = {
    val numEntries = inputScore.split(",").length().toDouble
    val retrievedEntry = (percentile * numEntries / 100.0 ).min(numEntries).max(0).toInt

    return inputScore
            .flatmap(scoreArray => scoreArray.split(",") )
            .map( score => score.toInt )
            .sortBy { case (score) => score }
            .zipWithIndex()
            .filter { case (score, index) => index == retrievedEntry }
            .map { case (score, index) => score }
            .collect()(0)
  }

}

object GK {

  def getGKStream() : RDD[String, Double] = {
    return getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase == "GK" )
            .filter( fields => fields(0) != "" && fields(14) != "" && fields(25) != ""
                               fields(48) != "" && fields(49) != "" && fields(50) != ""
                               fields(51) != "" && fields(52) != "" )
            .map( fields => Array(fields(0), fields(14) + "," + fields(15) + ","
                      + fields(25) + "," + fields(48) + "," + fields(49) + ","
                      + fields(50) + "," + fields(51) + "," + fields(52) ) )
            .map( ( x1, x2 ) =>  Array(x1, MyMath.getRddPercentile( MySparkContext
              .getSparkContext().parallelize( x2 ) ) ) )
            .sortBy( fields => fields(1),  ascending = false )
  }

}
