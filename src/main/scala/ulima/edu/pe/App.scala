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

    //Arqueros
    getGKs()

    //Defensas
    getCBs()
    getRWBs()
    getLBs()
    getLWBs()

    //Mediocampistas
    getCDMs()
    getCAMs()
    getLMs()
    getRMs()

    //Delanteros
    getCFs()
    getSTs()
  }

  def getGKs() {
    val max2 = getGKStream().take(2) //Porque son 2 jugadores de esta posicion
    MySparkContext.getSparkContext().parallelize(max2)
      .map( fields => fields(0) + "," + fields(1) )
      .saveAsTextFile("data/resultadoGK/") //folder donde se guarda data/resultado##
  }

  def getGKStream() : RDD[Array[String]] = {
    getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("GK"))
            //campos a filtrar
            .filter( fields => fields(0) != "" && fields(14) != "" &&
                               fields(25) != "" && fields(48) != "" &&
                               fields(49) != "" && fields(50) != "" &&
                               fields(51) != "" && fields(52) != "" )
             /* los mismos campos pero el
             Array es de (nombre de jugador, String de campos
             separados por coma)*/
            .map( fields => Array(fields(0), fields(14) + "," + fields(15) + ","
                      + fields(25) + "," + fields(48) + "," + fields(49) + ","
                      + fields(50) + "," + fields(51) + "," + fields(52) ) )
            .map( fields =>  Array(fields(0),
            MyMath.getRddPercentile( fields(1), 50 ).toString ) )
            .sortBy( fields => fields(1).toDouble, ascending = false )
  }

  def getCBs() {
    val max4 = getCBStream().take(2) //Porque son 4 jugadores de esta posicion
    MySparkContext.getSparkContext().parallelize(max4)
      .map( fields => fields(0) + "," + fields(1) )
      .saveAsTextFile("data/resultadoCB/") //folder donde se guarda data/resultado##
  }

  def getCBStream() : RDD[Array[String]] = {
    getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("CB"))
            //campos a filtrar
            .filter( fields => fields(0) != "" && fields(21) != "" &&
                               fields(22) != "" && fields(23) != "" &&
                               fields(24) != "" && fields(25) != "" &&
                               fields(27) != "" && fields(34) != "" &&
                               fields(36) != "" && fields(39) != "" )
            /* los mismos campos pero el
            Array es de (nombre de jugador, String de campos
            separados por coma)*/
            .map( fields => Array(fields(0), fields(21) + "," + fields(22) + ","
                      + fields(23) + "," + fields(24) + "," + fields(25) + ","
                      + fields(27) + "," + fields(34) + "," + fields(36) + ","
                      + fields(39) ) )
            .map( fields =>  Array(fields(0),
            MyMath.getRddPercentile( fields(1), 50 ).toString ) )
            .sortBy( fields => fields(1).toDouble, ascending = false )
  }

  def getRWBs() {
    val max4 = getRWBStream().take(2) //Porque son 4 jugadores de esta posicion
    MySparkContext.getSparkContext().parallelize(max4)
      .map( fields => fields(0) + "," + fields(1) )
      .saveAsTextFile("data/resultadoRWB/") //folder donde se guarda data/resultado##
  }

  def getRWBStream() : RDD[Array[String]] = {
    getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("RWB"))
            //campos a filtrar
            .filter( fields => fields(0) != "" && fields(19) != "" &&
                               fields(24) != "" && fields(31) != "" &&
                               fields(32) != "" && fields(21) != "" &&
                               fields(33) != "" && fields(34) != "" &&
                               fields(35) != "")
            /* los mismos campos pero el
            Array es de (nombre de jugador, String de campos
            separados por coma)*/
            .map( fields => Array(fields(0), fields(19) + "," + fields(24) + ","
                      + fields(31) + "," + fields(32) + "," + fields(21) + ","
                      + fields(33) + "," + fields(34) + "," + fields(35)) )
            .map( fields =>  Array(fields(0),
            MyMath.getRddPercentile( fields(1), 50 ).toString ) )
            .sortBy( fields => fields(1).toDouble, ascending = false )
  }

  def getLBs() {
    val max4 = getLBStream().take(2) //Porque son 2 jugadores de esta posicion
    MySparkContext.getSparkContext().parallelize(max4)
      .map( fields => fields(0) + "," + fields(1) )
      .saveAsTextFile("data/resultadoLB/") //folder donde se guarda data/resultado##
  }

  def getLBStream() : RDD[Array[String]] = {
    getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LB"))
            //campos a filtrar
            .filter( fields => fields(0) != "" && fields(19) != "" &&
                               fields(24) != "" && fields(31) != "" &&
                               fields(32) != "" && fields(21) != "" &&
                               fields(33) != "" && fields(34) != "" &&
                               fields(35) != "")
            /* los mismos campos pero el
            Array es de (nombre de jugador, String de campos
            separados por coma)*/
            .map( fields => Array(fields(0), fields(19) + "," + fields(24) + ","
                      + fields(31) + "," + fields(32) + "," + fields(21) + ","
                      + fields(33) + "," + fields(34) + "," + fields(35)) )
            .map( fields =>  Array(fields(0),
            MyMath.getRddPercentile( fields(1), 50 ).toString ) )
            .sortBy( fields => fields(1).toDouble, ascending = false )
  }

  def getLWBs() {
    val max4 = getLWBStream().take(2) //Porque son 2 jugadores de esta posicion
    MySparkContext.getSparkContext().parallelize(max4)
      .map( fields => fields(0) + "," + fields(1) )
      .saveAsTextFile("data/resultadoLWB/") //folder donde se guarda data/resultado##
  }

  def getLWBStream() : RDD[Array[String]] = {
    getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LWB"))
            //campos a filtrar
            .filter( fields => fields(0) != "" && fields(19) != "" &&
                               fields(24) != "" && fields(31) != "" &&
                               fields(32) != "" && fields(21) != "" &&
                               fields(33) != "" && fields(34) != "" &&
                               fields(35) != "")
            /* los mismos campos pero el
            Array es de (nombre de jugador, String de campos
            separados por coma)*/
            .map( fields => Array(fields(0), fields(19) + "," + fields(24) + ","
                      + fields(31) + "," + fields(32) + "," + fields(21) + ","
                      + fields(33) + "," + fields(34) + "," + fields(35)) )
            .map( fields =>  Array(fields(0),
            MyMath.getRddPercentile( fields(1), 50 ).toString ) )
            .sortBy( fields => fields(1).toDouble, ascending = false )
  }

  def getCFs() {
    val max2 = getCFStream().take(2) //Porque son 2 jugadores de esta posicion para 22 jugadores
    MySparkContext.getSparkContext().parallelize(max2)
      .map( fields => fields(0) + "," + fields(1) )
      .saveAsTextFile("data/resultadoCF/") //folder donde se guarda data/resultado##
  }

  def getCFStream() : RDD[Array[String]] = {
    getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("CF"))
            //campos a filtrar
            .filter( fields => fields(0) != "" && fields(19) != "" &&
                               fields(26) != "" && fields(34) != "" &&
                               fields(33) != "" && fields(37) != "" &&
                               fields(38) != "" && fields(39) != "" &&
                               fields(40) != "" && fields(42) != "" &&
                               fields(18) != "" && fields(20) != "")
             /* los mismos campos pero el
             Array es de (nombre de jugador, String de campos
             separados por coma)*/
            .map( fields => Array(fields(0), fields(19) + "," + fields(26) + ","
                      + fields(34) + "," + fields(33) + "," + fields(37) + ","
                      + fields(38) + "," + fields(39) + "," + fields(40) + ","
                      + fields(42) + "," + fields(18) + "," + fields(20) + ",") )
            .map( fields =>  Array(fields(0),
            MyMath.getRddPercentile( fields(1), 50 ).toString ) )
            .sortBy( fields => fields(1).toDouble, ascending = false )
  }

  def getSTs() {
    val max2 = getSTStream().take(2) //Porque son 2 jugadores de esta posicion para 22 jugadores
    MySparkContext.getSparkContext().parallelize(max2)
      .map( fields => fields(0) + "," + fields(1) )
      .saveAsTextFile("data/resultadoST/") //folder donde se guarda data/resultado##
  }

  def getSTStream() : RDD[Array[String]] = {
    getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("ST"))
            //campos a filtrar
            .filter( fields => fields(0) != "" && fields(19) != "" &&
                               fields(26) != "" && fields(34) != "" &&
                               fields(33) != "" && fields(37) != "" &&
                               fields(38) != "" && fields(39) != "" &&
                               fields(40) != "" && fields(42) != "" &&
                               fields(18) != "" && fields(20) != "")
             /* los mismos campos pero el
             Array es de (nombre de jugador, String de campos
             separados por coma)*/
             .map( fields => Array(fields(0), fields(19) + "," + fields(26) + ","
                       + fields(34) + "," + fields(33) + "," + fields(37) + ","
                       + fields(38) + "," + fields(39) + "," + fields(40) + ","
                       + fields(42) + "," + fields(18) + "," + fields(20) + ",") )
            .map( fields =>  Array(fields(0),
            MyMath.getRddPercentile( fields(1), 50 ).toString ) )
            .sortBy( fields => fields(1).toDouble, ascending = false )
  }

  def getCDMs() {
    val max4 = getCDMStream().take(2) //Porque son 2 jugadores de esta posicion
    MySparkContext.getSparkContext().parallelize(max4)
      .map( fields => fields(0) + "," + fields(1) )
      .saveAsTextFile("data/resultadoCDM/") //folder donde se guarda data/resultado##
  }

  def getCDMStream() : RDD[Array[String]] = {
    getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("CDM"))
            //campos a filtrar
            .filter( fields => fields(0) != "" && fields(27) != "" &&
                               fields(35) != "" && fields(22) != "" &&
                               fields(21) != "" && fields(31) != "" &&
                               fields(19) != "" )
            /* los mismos campos pero el
            Array es de (nombre de jugador, String de campos
            separados por coma)*/
            .map( fields => Array(fields(0), fields(27) + "," + fields(35) + ","
                      + fields(22) + "," + fields(21) + "," + fields(31) + ","
                      + fields(19)) )
            .map( fields =>  Array(fields(0),
            MyMath.getRddPercentile( fields(1), 50 ).toString ) )
            .sortBy( fields => fields(1).toDouble, ascending = false )
  }

  def getCAMs() {
    val max4 = getCAMStream().take(2) //Porque son 2 jugadores de esta posicion
    MySparkContext.getSparkContext().parallelize(max4)
      .map( fields => fields(0) + "," + fields(1) )
      .saveAsTextFile("data/resultadoCAM/") //folder donde se guarda data/resultado##
  }

  def getCAMStream() : RDD[Array[String]] = {
    getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("CAM"))
            //campos a filtrar
            .filter( fields => fields(0) != "" && fields(27) != "" &&
                               fields(35) != "" && fields(22) != "" &&
                               fields(21) != "" && fields(31) != "" &&
                               fields(19) != "" )
            /* los mismos campos pero el
            Array es de (nombre de jugador, String de campos
            separados por coma)*/
            .map( fields => Array(fields(0), fields(27) + "," + fields(35) + ","
                      + fields(22) + "," + fields(21) + "," + fields(31) + ","
                      + fields(19)) )
            .map( fields =>  Array(fields(0),
            MyMath.getRddPercentile( fields(1), 50 ).toString ) )
            .sortBy( fields => fields(1).toDouble, ascending = false )
  }

  def getLMs() {
    val max4 = getLMStream().take(2) //Porque son 2 jugadores de esta posicion
    MySparkContext.getSparkContext().parallelize(max4)
      .map( fields => fields(0) + "," + fields(1) )
      .saveAsTextFile("data/resultadoLM/") //folder donde se guarda data/resultado##
  }

  def getLMStream() : RDD[Array[String]] = {
    getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LM"))
            //campos a filtrar
            .filter( fields => fields(0) != "" && fields(34) != "" &&
                               fields(33) != "" && fields(19) != "" &&
                               fields(18) != "" && fields(20) != "" &&
                               fields(38) != "" && fields(35) != "" &&
                               fields(26) != "")
            /* los mismos campos pero el
            Array es de (nombre de jugador, String de campos
            separados por coma)*/
            .map( fields => Array(fields(0), fields(34) + "," + fields(33) + ","
                      + fields(19) + "," + fields(18) + "," + fields(20) + ","
                      + fields(38) + "," + fields(35) + "," + fields(26) ) )
            .map( fields =>  Array(fields(0),
            MyMath.getRddPercentile( fields(1), 50 ).toString ) )
            .sortBy( fields => fields(1).toDouble, ascending = false )
  }

  def getRMs() {
    val max4 = getRMStream().take(2) //Porque son 2 jugadores de esta posicion
    MySparkContext.getSparkContext().parallelize(max4)
      .map( fields => fields(0) + "," + fields(1) )
      .saveAsTextFile("data/resultadoRM/") //folder donde se guarda data/resultado##
  }

  def getRMStream() : RDD[Array[String]] = {
    getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("RM"))
            //campos a filtrar
            .filter( fields => fields(0) != "" && fields(34) != "" &&
                               fields(33) != "" && fields(19) != "" &&
                               fields(18) != "" && fields(20) != "" &&
                               fields(38) != "" && fields(35) != "" &&
                               fields(26) != "")
            /* los mismos campos pero el
            Array es de (nombre de jugador, String de campos
            separados por coma)*/
            .map( fields => Array(fields(0), fields(34) + "," + fields(33) + ","
                      + fields(19) + "," + fields(18) + "," + fields(20) + ","
                      + fields(38) + "," + fields(35) + "," + fields(26) ) )
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
