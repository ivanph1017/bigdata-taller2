package ulima.edu.pe

import org.apache.spark.rdd.RDD

/**
 * @author ${user.name}
 */
object GK {

  /*def getGKStream(playerArray : RDD[Array[String]]) : RDD[Array[String]] = {
    return playerArray.filter( fields => fields(15).toUpperCase == "GK" )
               .filter( fields => fields(0) != "" && fields(14) != "" && fields(25) != "" )
               .map( fields => Array( fields(0), fields(14), fields(15), fields(25) ) )
               .orderBy( fields(25),  ascending = true )
               .sample( false, 2 / playerArray.count() )
  }*/

}
