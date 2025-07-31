package com.gable.templar.zeus.custom

import com.fasterxml.jackson.databind.JsonNode
import com.gable.templar.constant.JobConstant.{JOB_TYPE, initialTitle}
import com.gable.templar.custom.view.{DependencyCheckModel, NotebookIdAddParameterRequest, NotebookRunParallelRequest}
import com.gable.templar.heaven.exception.InvalidArgumentException
import com.gable.templar.heaven.util.{HTTPServletRequestUtil, RestTemplateFactoryUtil}
import com.gable.templar.zeus.controller.model.LoginUser
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.springframework.web.context.request.{RequestContextHolder, ServletRequestAttributes}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util
import java.util.Map
import scala.util.{Failure, Success, Try}

trait CustomFw {

  var schemaName = ""

  def doRunFramework(dependencyCheckModel: DependencyCheckModel,
                     JOB_TYPE: JOB_TYPE,sparkSession: SparkSession,loginUser: LoginUser): Unit

  def checkDependencyByJobName(dependencyCheckModel: DependencyCheckModel,sparkSession: SparkSession): java.util.Map[String,Boolean]

  def setSchemaName(schemaName:String): Unit = {
    this.schemaName = schemaName
  }

  def doRunNotebookParallel(param: util.HashMap[String,JsonNode], notebookId: String,dependencyCheckModel: DependencyCheckModel,loginUser: LoginUser): Unit = {
    var runId: String = null
    if(dependencyCheckModel.get_workflowId() != null) {
      runId = dependencyCheckModel.get_workflowId() + "||" + dependencyCheckModel.get_runId() + "||" + dependencyCheckModel.get_taskId()
    }
    else {
      runId = "manual_run_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HH_mm_ss"))
    }
    val attrs = RequestContextHolder.getRequestAttributes.asInstanceOf[ServletRequestAttributes]
    val token = HTTPServletRequestUtil.getToken(attrs.getRequest)
    val notebookRunParallelRequest = new NotebookRunParallelRequest
    notebookRunParallelRequest.setRunBy(loginUser.getUsername)
    notebookRunParallelRequest.setAsync(true)
    notebookRunParallelRequest.setRunningId(runId)
    notebookRunParallelRequest.setLanguage("python")
    val notebookIdAddParameterRequest: NotebookIdAddParameterRequest = new NotebookIdAddParameterRequest
    val addParameterRequest: util.Map[String, util.Map[String, JsonNode]] = new util.HashMap[String,util.Map[String,JsonNode]]()
    addParameterRequest.put(initialTitle,param)
    notebookIdAddParameterRequest.setNotebookId(notebookId)
    notebookIdAddParameterRequest.setAddParameterMapFromTitle(addParameterRequest)
    RestTemplateFactoryUtil.getRestTemplar(token).postForObject("/")
  }

  def checkOrderDatetimeFormat(patternDate: String): Boolean = {
    var useBetweenFlag = true

    def checkIndexAndFound(formatIn: String, charFind: String): Int = {
      var maxIndexOnFind: Int = -1
      var tempFormatIn = formatIn
      var currentOffset = 0 // Keep track of characters removed

      var foundIndex = tempFormatIn.indexOf(charFind)
      while (foundIndex != -1) {
        maxIndexOnFind = currentOffset + foundIndex
        tempFormatIn = tempFormatIn.replaceFirst(java.util.regex.Pattern.quote(charFind), "")
        currentOffset += charFind.length // Account for removed characters when updating offset
        foundIndex = tempFormatIn.indexOf(charFind)
      }
      maxIndexOnFind
    }

    var current_index = 0
    var patternDateTemp = patternDate.replace("%%", "") // Handle escaped '%'
    val listOrderToCheck: List[Any] = List(
      List("%Y", "%y"), // Year (full, abbreviated)
      List("%U", "%W"), // Week number (Sunday start, Monday start)
      "%m",            // Month
      "%w",            // Weekday (0-6)
      "%d",            // Day of month
      "%H",            // Hour (24-hour)
      "%M",            // Minute
      "%S",            // Second
      "%f"             // Microsecond
    )

    for (itemIn <- listOrderToCheck) {
      if (!useBetweenFlag) { // Early exit if flag is already false
        return false
      }

      itemIn match {
        case items: List[_] => // Handle lists of alternative formats (e.g., %Y or %y)
          var checkingLoopCount = 0
          var foundInSublist = false
          for (item <- items.map(_.asInstanceOf[String])) {
            val indexOut = checkIndexAndFound(patternDateTemp, item)
            if (indexOut != -1) {
              foundInSublist = true // Mark that at least one item from the sublist was found
              if (indexOut >= current_index) {
                current_index = indexOut
              } else {
                if (checkingLoopCount == 0) { // Only set false if it's the first relevant item out of order
                  useBetweenFlag = false
                  // Break logic is handled by the outer loop's check on useBetweenFlag
                }
              }
              // Replace the found item to ensure next search is accurate relative to remaining string
              patternDateTemp = patternDateTemp.replaceFirst(java.util.regex.Pattern.quote(item), "")
              // No need to continue checking other items in this sublist if one is found and processed
              // However, the original Python code continues iterating the inner loop, so we'll mimic that.
            }
            checkingLoopCount += 1
          }
        // If none of the sublist items were found, treat it as "not found" for this level
        // The Python `pass` effectively means no change to useBetweenFlag if not found
        // The critical part for `useBetweenFlag = false` is `index_out < current_index`
        // If no match in sublist, `current_index` doesn't change, which is correct.

        case item: String => // Handle single format specifiers
          val indexOut = checkIndexAndFound(patternDateTemp, item)
          if (indexOut == -1) {
            // Can't Find Any Format - do nothing, as in Python's pass
          } else if (indexOut >= current_index) {
            current_index = indexOut
          } else if (indexOut < current_index) {
            useBetweenFlag = false
          }
          patternDateTemp = patternDateTemp.replaceFirst(java.util.regex.Pattern.quote(item), "")
      }
    }

    // After checking all ordered formats, if any '%' remains, it's an unhandled format
    if (patternDateTemp.contains('%')) {
      useBetweenFlag = false
    }

    println(s"Status Check Flag : $useBetweenFlag")
    useBetweenFlag
  }


  def minusDateByFrequency(dateIn: LocalDateTime, frequency: String, valueFeq: Any = 1): LocalDateTime = {
    var effectiveValueFeq = 0
    if (valueFeq == null || valueFeq.toString.trim.isEmpty) {
      effectiveValueFeq = 0
    } else {
      Try(valueFeq.toString.toInt) match {
        case Success(v) => effectiveValueFeq = v
        case Failure(e) =>
          val errMsg = s"Has Error on config value to calculate date is number $valueFeq. Detail error -> ${e.getMessage}"
          throw new InvalidArgumentException(errMsg)
      }
    }

    var effectiveFrequency = frequency
    if (effectiveFrequency == null || effectiveFrequency.trim.isEmpty) {
      effectiveFrequency = "daily"
    }
    effectiveFrequency = effectiveFrequency.toLowerCase.trim

    if (effectiveFrequency.endsWith("m") && effectiveFrequency.length > 1) { // Check for "Xm" (minutes)
      val valueInStr = effectiveFrequency.dropRight(1).trim
      if (valueInStr.forall(Character.isDigit)) {
        val valueIn = valueInStr.toInt
        dateIn.minus(valueIn.toLong * effectiveValueFeq, ChronoUnit.MINUTES)
      } else {
        val errMsg = s"Has Something Wrong with frequency $frequency"
        println(errMsg)
        throw new InvalidArgumentException(errMsg)
      }
    } else if (effectiveFrequency.endsWith("h") && effectiveFrequency.length > 1) { // Check for "Xh" (hours)
      val valueInStr = effectiveFrequency.dropRight(1).trim
      if (valueInStr.forall(Character.isDigit)) {
        val valueIn = valueInStr.toInt
        dateIn.minus(valueIn.toLong * effectiveValueFeq, ChronoUnit.HOURS)
      } else {
        val errMsg = s"Has Something Wrong with frequency $frequency"
        println(errMsg)
        throw new InvalidArgumentException(errMsg)
      }
    } else if (effectiveFrequency == "hourly") {
      dateIn.minus(1L * effectiveValueFeq, ChronoUnit.HOURS)
    } else if (effectiveFrequency == "daily") {
      dateIn.minus(1L * effectiveValueFeq, ChronoUnit.DAYS)
    } else if (effectiveFrequency == "weekly") {
      dateIn.minus(7L * effectiveValueFeq, ChronoUnit.DAYS)
    } else if (effectiveFrequency == "monthly") {
      dateIn.minus(1L * effectiveValueFeq, ChronoUnit.MONTHS)
    } else if (effectiveFrequency == "quarterly") {
      dateIn.minus(3L * effectiveValueFeq, ChronoUnit.MONTHS)
    } else if (effectiveFrequency == "yearly") {
      dateIn.minus(1L * effectiveValueFeq, ChronoUnit.YEARS)
    } else {
      val errMsg = s"Do not know how this frequency work: $frequency"
      println(errMsg)
      throw new InvalidArgumentException(errMsg)
    }
  }

  def addDateByFrequency(dateIn: LocalDateTime, frequency: String, valueFeq: Any = 1): LocalDateTime = {
    var effectiveValueFeq = 0
    if (valueFeq == null || valueFeq.toString.trim.isEmpty) {
      effectiveValueFeq = 0
    } else {
      Try(valueFeq.toString.toInt) match {
        case Success(v) => effectiveValueFeq = v
        case Failure(e) =>
          val errMsg = s"Has Error on config value to calculate date is number $valueFeq. Detail error -> ${e.getMessage}"
          throw new InvalidArgumentException(errMsg)
      }
    }

    var effectiveFrequency = frequency
    if (effectiveFrequency == null || effectiveFrequency.trim.isEmpty) {
      effectiveFrequency = "daily"
    }
    effectiveFrequency = effectiveFrequency.toLowerCase.trim

    if (effectiveFrequency.endsWith("m") && effectiveFrequency.length > 1) { // Check for "Xm" (minutes)
      val valueInStr = effectiveFrequency.dropRight(1).trim // Remove 'm' and trim
      if (valueInStr.forall(Character.isDigit)) {
        val valueIn = valueInStr.toInt
        dateIn.plus(valueIn.toLong * effectiveValueFeq, ChronoUnit.MINUTES)
      } else {
        val errMsg = s"Has Something Wrong with frequency $frequency"
        println(errMsg)
        throw new InvalidArgumentException(errMsg)
      }
    } else if (effectiveFrequency.endsWith("h") && effectiveFrequency.length > 1) { // Check for "Xh" (hours)
      val valueInStr = effectiveFrequency.dropRight(1).trim
      if (valueInStr.forall(Character.isDigit)) {
        val valueIn = valueInStr.toInt
        dateIn.plus(valueIn.toLong * effectiveValueFeq, ChronoUnit.HOURS)
      } else {
        val errMsg = s"Has Something Wrong with frequency $frequency"
        println(errMsg)
        throw new InvalidArgumentException(errMsg)
      }
    } else if (effectiveFrequency == "hourly") {
      dateIn.plus(1L * effectiveValueFeq, ChronoUnit.HOURS)
    } else if (effectiveFrequency == "daily") {
      dateIn.plus(1L * effectiveValueFeq, ChronoUnit.DAYS)
    } else if (effectiveFrequency == "weekly") {
      dateIn.plus(7L * effectiveValueFeq, ChronoUnit.DAYS)
    } else if (effectiveFrequency == "monthly") {
      dateIn.plus(1L * effectiveValueFeq, ChronoUnit.MONTHS)
    } else if (effectiveFrequency == "quarterly") {
      dateIn.plus(3L * effectiveValueFeq, ChronoUnit.MONTHS)
    } else if (effectiveFrequency == "yearly") {
      dateIn.plus(1L * effectiveValueFeq, ChronoUnit.YEARS)
    } else {
      val errMsg = s"Do not know how this frequency work: $frequency"
      println(errMsg)
      throw new InvalidArgumentException(errMsg)
    }
  }

  def postgresqlQueryFunc(
                           server: String,
                           port: String, // Include port in function signature as per Python, use in URL
                           dbName: String, // Renamed from db_name for Scala convention
                           username: String,
                           password: String,
                           query: String,
                           sparkSession: SparkSession
                         ): DataFrame = {
    println(s"Query : $query")
    try {
      val jdbcUrl = s"jdbc:postgresql://$server:$port/$dbName" // Including port for standard JDBC URL
      val spdf = sparkSession.read.format("jdbc") // Use GlobalConfig.spark if integrating
        .option("url", jdbcUrl)
        .option("query", query) // "query" option is used for arbitrary SQL queries
        .option("user", username)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        // These options are PostgreSQL JDBC driver specific for SSL/TLS connections
        // and are correctly applied in Scala as well.
        .option("encrypt", "true") // Note: Some drivers might use `ssl` instead of `encrypt`
        .option("trustServerCertificate", "true") // Note: Some drivers might use `sslmode=require` or similar.
        // "trustServerCertificate" is more common with SQL Server JDBC.
        // For PostgreSQL, `sslmode=verify-full` or `sslmode=require` are typical.
        // Double-check your PostgreSQL driver's SSL options.
        .load()

      println("Complete Query")
      spdf
    } catch {
      case e: Exception =>
        // Original Python printed partial error message and then re-raised.
        // Scala can directly re-throw the custom exception with the original cause.
        println(s"An error occurred during PostgreSQL query: ${e.getMessage}")
        throw new Exception(e) // Pass the original exception as the cause
    }
  }

  def convertPythonDateFormatToJava(pythonPattern: String): String = {
    pythonPattern
      .replace("%Y", "yyyy")
      .replace("%m", "MM")
      .replace("%d", "dd")
      .replace("%H", "HH")
      .replace("%M", "mm")
      .replace("%S", "ss")
      .replace("%f", "SSSSSS") // Milliseconds or nanoseconds depending on precision
      .replace("%%", "%")     // Handle escaped %
    // ... add more conversions as needed (e.g., %y, %U, %W, %w)
  }

  def checkBusinessColumn(
                           frequencyCheck: String,
                           businessColumn: String,
                           prerequisiteSchema: String,
                           prerequisiteTable: String,
                           targetDate: String,
                           sparkSession: SparkSession
                         ): Array[Row] = {

    val queryTableSourceCheck = Map(
      "tbl_source_1_day" ->
        s"""SELECT `$businessColumn` FROM $prerequisiteSchema.$prerequisiteTable WHERE `$businessColumn` = '$targetDate' GROUP BY `$businessColumn`""",
      "tbl_source_multi_day" ->
        s"""SELECT `$businessColumn` FROM $prerequisiteSchema.$prerequisiteTable WHERE `$businessColumn` $targetDate GROUP BY `$businessColumn`""",
      "tbl_source_max_day" ->
        s"""SELECT `$businessColumn` FROM $prerequisiteSchema.$prerequisiteTable GROUP BY `$businessColumn`""",
      "tbl_source_max_day_between" ->
        s"""SELECT `$businessColumn` FROM $prerequisiteSchema.$prerequisiteTable WHERE `$businessColumn` >= '$targetDate' GROUP BY `$businessColumn`"""
    )

    // Determine the query to use based on frequencyCheck
    val query = frequencyCheck match {
      case fc if Set("daily", "weekly", "cur_month", "prev_month", "eom", "day-n", "hourly").contains(fc) =>
        queryTableSourceCheck("tbl_source_1_day")
      case fc if Set(
        "quarter", "month_to_date", "hour_to_date", "date_range", "hour_in_range",
        "daily_period", "date_in_range", "year_to_date", "24hours", "every_n_hour"
      ).contains(fc) =>
        queryTableSourceCheck("tbl_source_multi_day")
      case "max_date" =>
        queryTableSourceCheck("tbl_source_max_day")
      case "max_date_between" =>
        queryTableSourceCheck("tbl_source_max_day_between")
      case _ =>
        throw new IllegalArgumentException(s"Unsupported frequency check type: $frequencyCheck")
    }

    println(s"Query On Data Source: $query")

    val records = sparkSession.sql(query).collect()
    records
  }

}
