package com.gable.templar.zeus.custom

import com.fasterxml.jackson.databind.ObjectMapper
import com.gable.templar.heaven.exception.InvalidArgumentException
import com.gable.templar.zeus.SparkServer
import com.gable.templar.zeus.service.vector.ConnectionInfo
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

object ConnectionService {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val objectMapper: ObjectMapper = new ObjectMapper()

  final case class QueryHandle(conn: Connection, stmt: PreparedStatement, rs: ResultSet) extends AutoCloseable {
    override def close(): Unit = {
      try rs.close() finally try stmt.close() finally conn.close()
    }
  }

  def postgresqlQueryDirectly(server: String,
                              port: String, // Include port in function signature as per Python, use in URL
                              dbName: String, // Renamed from db_name for Scala convention
                              username: String,
                              password: String,
                              query: String): QueryHandle = {
    val url = s"jdbc:postgresql://$server:$port/$dbName"
    logger.info("Executing query: {}", query)

    val connection = DriverManager.getConnection(url, username, password)
    val statement = connection.prepareStatement(query)
    QueryHandle(connection, statement, statement.executeQuery())
  }

//  def postgresqlQueryFunc(
//                           server: String,
//                           port: String, // Include port in function signature as per Python, use in URL
//                           dbName: String, // Renamed from db_name for Scala convention
//                           username: String,
//                           password: String,
//                           query: String,
//                           sparkSession: SparkSession
//                         ): DataFrame = {
//    println(s"Query : $query")
//    try {
//      val jdbcUrl = s"jdbc:postgresql://$server:$port/$dbName" // Including port for standard JDBC URL
//      val spdf = sparkSession.read.format("jdbc") // Use GlobalConfig.spark if integrating
//        .option("url", jdbcUrl)
//        .option("query", query) // "query" option is used for arbitrary SQL queries
//        .option("user", username)
//        .option("password", password)
//        .option("driver", "org.postgresql.Driver")
//        // These options are PostgreSQL JDBC driver specific for SSL/TLS connections
//        // and are correctly applied in Scala as well.
//        .option("encrypt", "true") // Note: Some drivers might use `ssl` instead of `encrypt`
//        .option("trustServerCertificate", "true") // Note: Some drivers might use `sslmode=require` or similar.
//        // "trustServerCertificate" is more common with SQL Server JDBC.
//        // For PostgreSQL, `sslmode=verify-full` or `sslmode=require` are typical.
//        // Double-check your PostgreSQL driver's SSL options.
//        .load()
//
//      println("Complete Query")
//      spdf
//    } catch {
//      case e: Exception =>
//        // Original Python printed partial error message and then re-raised.
//        // Scala can directly re-throw the custom exception with the original cause.
//        println(s"An error occurred during PostgreSQL query: ${e.getMessage}")
//        throw new Exception(e) // Pass the original exception as the cause
//    }
//  }


  def postgresqlInsertUpdateFunc(
                                  server: String,
                                  port: String,
                                  dbName: String,
                                  username: String,
                                  password: String,
                                  query: String,                   // SQL with ? placeholders
                                  params: Seq[Any] = Seq.empty    // Parameters to bind to ?
                                ): Unit = {
    val url = s"jdbc:postgresql://$server:$port/$dbName"
    logger.info("Executing query: {}", query)
    logger.info("log check param: {}",params)

    val connection = DriverManager.getConnection(url, username, password)
    val statement = connection.prepareStatement(query)

    try {
      // Bind each parameter to the prepared statement
      for ((param, index) <- params.zipWithIndex) {
        param match {
          case s: String => statement.setString(index + 1, s)
          case i: Int    => statement.setInt(index + 1, i)
          case l: Long   => statement.setLong(index + 1, l)
          case d: Double => statement.setDouble(index + 1, d)
          case t: java.sql.Timestamp => statement.setTimestamp(index + 1, t)
          case dt: java.time.LocalDateTime => statement.setTimestamp(index + 1, java.sql.Timestamp.valueOf(dt))
          case null => statement.setObject(index + 1, null)
          case other =>
            throw new IllegalArgumentException(s"Unsupported param type: ${other.getClass}")
        }
      }
      statement.executeUpdate()
    } finally {
      statement.close()
      connection.close()
    }
  }



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
      configMap.getOrElse(key, null)
    }

    val connectionInfo = new ConnectionInfo

    connectionInfo.setIp(getValue("ip"))
    connectionInfo.setPort(getValue("port"))
    connectionInfo.setDbName(getValue("db_name"))
    connectionInfo.setUserNm(getValue("user_nm"))
    if(getValue("password") != null) {
      var password = EncryptDecrypt.decrypt(getValue("password"))
      if(password != null)
        password = objectMapper.readTree(password).get("value").asText()
      connectionInfo.setPassword(password)
    }
    connectionInfo.setDbType(getValue("db_type"))
    connectionInfo.setSid(getValue("SID"))

    println("Query Complete")

    connectionInfo
  }

}
