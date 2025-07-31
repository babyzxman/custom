package com.gable.templar.zeus.custom

import com.gable.templar.heaven.exception.InvalidArgumentException
import com.gable.templar.zeus.SparkServer
import com.gable.templar.zeus.service.vector.ConnectionInfo

object ConnectionService {



  def getMasterConfigLog(queryMasterConfig: String,salt: String,key:String): ConnectionInfo = {
    println(s"Query Master in log process : $queryMasterConfig")

    // Execute the query using Spark SQL
    val configMstLogDf = SparkServer.getZeusSession().session.sql(queryMasterConfig)

    // Extract values directly from Spark DataFrame without converting to Pandas
    // Use .head() to get the first (and presumably only) row after filtering
    // Use .getString(0) assuming 'values' column is at index 0 in the selected data
    // Or, more robustly, use .select(col("values")).as[String].head() if you have a Dataset
    // Or, even better, map to a case class if the schema is fixed and known.

    // For this example, we'll use a safer approach with .collect() and .find()
    // to handle cases where a key might not be found.

    val configMap = configMstLogDf
      .select("key", "values") // Ensure you select the 'key' and 'values' columns
      .collect() // Collect rows to the driver. Be cautious with large results.
      .map(row => row.getString(0) -> row.getString(1)) // Map to key -> value pair
      .toMap // Convert to a Map for easy lookup

    def getValue(key: String): String = {
      configMap.getOrElse(key, {
        val errMsg = s"Configuration key '$key' not found in master config."
        println(errMsg)
        throw new InvalidArgumentException(errMsg)
      })
    }

    val connectionInfo = new ConnectionInfo

    connectionInfo.setIp(getValue("ip"))
    connectionInfo.setPort(getValue("port"))
    connectionInfo.setDbName(getValue("db_name"))
    connectionInfo.setUserNm(getValue("user_nm"))
    connectionInfo.setPassword("K3Z0bHMnHhE6HeinZ3uXda6y1TRJEN")
    connectionInfo.setDbType(getValue("db_type"))

    println("Query Complete")

    connectionInfo
  }

}
