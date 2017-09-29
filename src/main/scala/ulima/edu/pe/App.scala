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

  def process() {
    val max2 = getGKStream().take(2)
    MySparkContext.getSparkContext().parallelize(max2)
      .map( fields => fields(0) + "," + fields(1) + "," +
            fields(2) + "," + fields(3) )
      .saveAsTextFile("data/resultadoGK/")
  }

  def main(args : Array[String]) {
    process()
  }

  def getGKStream() : RDD[Array[String]] = {
    return getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase == "GK" )
            .filter( fields => fields(0) != "" && fields(14) != "" && fields(25) != "" )
            .map( fields => Array( fields(0), fields(14), fields(15), fields(25) ) )
            .sortBy( fields => fields(3).toInt,  ascending = false )
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
