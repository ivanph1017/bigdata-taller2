package ulima.edu.pe

import org.apache.spark.rdd.RDD

/**
 * @author ${user.name}
 */

 object CB {

   /*val playerArray

   def getGKStream(playerArray : RDD[Array[String]]) : RDD[Array[String]] = {
     this.playerArray = playerArray
     filterGK()
   }

   def filterGKEmptyFields() : RDD[Array[String]] = {
     playerArray.filter(fields => fields(15).toUpperCase == "GK")
                     .filter( fields => fields(0) != "" && fields(14) != ""
                       && fields(25) != "" )
   }

   def mapGK(playerArray : RDD[Array[String]]) : RDD[Array[String]] = {
     playerTuple.map( fields => Array( fields(0), fields(14), fields(15), fields(25) ) )
   }

   def filterGK(playerArray : RDD[Array[String]]) : RDD[Array[String]] = {
     playerArray.orderBy( fields(25),  ascending = true)
     .sample(false, 2 / playerArray.count())
   }*/

 }
