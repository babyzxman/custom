//package com.gable.templar.zeus.custom
//
//import java.io.{BufferedWriter, OutputStreamWriter}
//import java.net.{HttpURLConnection, URL}
//import java.nio.charset.StandardCharsets
//import java.time.{ZoneId, ZonedDateTime}
//import java.time.format.DateTimeFormatter
//import java.util.Properties
//import jakarta.mail.{Message, Session, Transport}
//import jakarta.mail.internet.{InternetAddress, MimeMessage}
//
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
//// Custom wrapper so callers can distinguish notification failures
//final case class SentNotificationError(cause: Throwable) extends Exception(cause)
//
//object Notifier {
//
//  /** Formats now() in Asia/Bangkok, like 2025-08-22 13:37:00 */
//  private def nowBKK(): String = {
//    val zdt = ZonedDateTime.now(ZoneId.of("Asia/Bangkok"))
//    zdt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
//  }
//
//  // ----------------------- LINE Notify -----------------------
//  // API: POST https://notify-api.line.me/api/notify  with x-www-form-urlencoded { message }
//  def lineNotify(
//                  jobName: String,
//                  status: String,
//                  errMsgIn: String,
//                  rawTable: String,
//                  token: String,
//                  mode: String
//                ): Unit = {
//    val errMsg = Option(errMsgIn).filterNot(m => m == null || m.equalsIgnoreCase("null")).getOrElse("-")
//    val rawTbl = Option(rawTable).getOrElse("-")
//
//    val url = new URL("https://notify-api.line.me/api/notify")
//    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
//    conn.setRequestMethod("POST")
//    conn.setDoOutput(true)
//    conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
//    conn.setRequestProperty("Authorization", s"Bearer $token")
//
//    val message =
//      s"""
//         |Notification Type : %s
//         |Jobname : %s
//         |Status : %s
//         |ErrorMessage : %s
//         |TableName : %s
//         |Date : %s
//         |""".stripMargin.format(mode, jobName, status, errMsg, rawTbl, nowBKK()).trim
//
//    val body = "message=" + java.net.URLEncoder.encode(message, "UTF-8")
//    val out = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream, StandardCharsets.UTF_8))
//    out.write(body)
//    out.flush()
//    out.close()
//
//    val code = conn.getResponseCode
//    if (code / 100 != 2) throw new RuntimeException(s"LINE Notify HTTP $code")
//  }
//
//  // ----------------------- Microsoft Teams Webhook -----------------------
//  // Sends a simple JSON payload {"text": "..."} to an Incoming Webhook URL
//  def msteamNotify(
//                    jobName: String,
//                    rawTable: String,
//                    mode: String,
//                    errMsg: String,
//                    status: String,
//                    webHookUrl: String
//                  ): Unit = {
//    val url = new URL(webHookUrl)
//    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
//    conn.setRequestMethod("POST")
//    conn.setDoOutput(true)
//    conn.setRequestProperty("Content-Type", "application/json; charset=utf-8")
//
//    val text =
//      Seq(
//        "JOB MS TEAM NOTIFICATION",
//        s"- Notification Type: $mode",
//        s"- Jobname: $jobName",
//        s"- Status: $status",
//        s"- ErrorMessage: ${Option(errMsg).getOrElse("-")}",
//        s"- TableName: ${Option(rawTable).getOrElse("-")}",
//        s"- Date: ${nowBKK()}"
//      ).mkString("\n")
//
//    val payload = s"""{ "text": ${escapeJson(text)} }"""
//
//    val out = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream, StandardCharsets.UTF_8))
//    out.write(payload)
//    out.flush()
//    out.close()
//
//    val code = conn.getResponseCode
//    if (code / 100 != 2) throw new RuntimeException(s"Teams webhook HTTP $code")
//  }
//
//  private def escapeJson(s: String): String = {
//    // very small JSON string escaper, wraps output in quotes
//    val b = new StringBuilder("\"")
//    s.foreach {
//      case '"' => b.append("\\\"")
//      case '\\' => b.append("\\\\")
//      case '\n' => b.append("\\n")
//      case '\r' => b.append("\\r")
//      case '\t' => b.append("\\t")
//      case c if c < ' ' => b.append(f"\\u${c.toInt}%04x")
//      case c => b.append(c)
//    }
//    b.append('"').toString
//  }
//
//  // ----------------------- Email (SMTP/Office365) -----------------------
//  def emailNotify(
//                   jobName: String,
//                   rawTable: String,
//                   mode: String,
//                   errMsg: String,
//                   status: String,
//                   sender: String,
//                   password: String,
//                   recipients: String
//                 ): Unit = {
//    val ok = Option(errMsg).forall(_ == "-")
//
//    val subject = "Status run job"
//    val statusHtml = if (ok) "<b style=\"color:green\">SUCCEED</b>" else "<b style=\"color:red\">FAILED</b>"
//
//    val body =
//      s"""
//         |<html>
//         |<body>
//         |  <p><b>JOB EMAIL NOTIFICATION</b></p>
//         |  <p>
//         |  <br>########################
//         |  <br><b>Notification Type</b> : $mode
//         |  <br><b>Jobname</b> : $jobName
//         |  <br><b>Status</b> : $statusHtml
//         |  <br><b>ErrorMessage</b> : ${Option(errMsg).getOrElse("-")}
//         |  <br><b>TableName</b> : ${Option(rawTable).getOrElse("-")}
//         |  <br><b>Date</b> : ${nowBKK()}
//         |  <br>########################
//         |  </p>
//         |</body>
//         |</html>
//         |""".stripMargin
//
//    val props = new Properties()
//    props.put("mail.smtp.auth", "true")
//    props.put("mail.smtp.starttls.enable", "true")
//    props.put("mail.smtp.host", "smtp.office365.com")
//    props.put("mail.smtp.port", "587")
//
//    val session = Session.getInstance(props, new jakarta.mail.Authenticator {
//      override protected def getPasswordAuthentication = new jakarta.mail.PasswordAuthentication(sender, password)
//    })
//
//    val message = new MimeMessage(session)
//    message.setFrom(new InternetAddress(sender))
//    message.setRecipients(Message.RecipientType.TO, recipients)
//    message.setSubject(subject, "UTF-8")
//    message.setContent(body, "text/html; charset=UTF-8")
//
//    Transport.send(message)
//  }
//
//  // ----------------------- Error grouping -----------------------
//  /**
//   * Tries to classify error group similarly to the Python version.
//   * Returns (group, system) where system is "spark" or "python" (here: "jvm").
//   */
//  def errGrouping(e: Throwable): (String, String) = {
//    val errModule = Option(e.getClass.getPackage).map(_.getName).getOrElse("")
//    val errClass = e.getClass.getSimpleName
//    val trace = org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace(e)
//
//    val (group, system) =
//      if (errModule.toLowerCase.contains("spark")) {
//        // Try to find text within [ ... ] on the same line as the exception class
//        val idx = trace.indexOf(errClass)
//        val bracket = if (idx >= 0) trace.substring(idx) else trace
//        val start = bracket.indexOf('[')
//        val end = bracket.indexOf(']')
//        val g1 = if (start >= 0 && end > start) bracket.substring(start + 1, end)
//        else if (trace.toLowerCase.contains("executor")) {
//          val exIdx = trace.toLowerCase.indexOf("executor")
//          val colon1 = trace.indexOf(':', exIdx)
//          val colon2 = if (colon1 >= 0) trace.indexOf(':', colon1 + 1) else -1
//          val part = if (colon1 >= 0 && colon2 > colon1) trace.substring(colon1 + 1, colon2) else errClass
//          part.split('.').last.trim
//        } else errClass
//        (g1, "spark")
//      } else (errClass, "jvm")
//
//    println(s"Class On Exception : $errClass")
//    println(s"$errModule : $errClass")
//    println(s"Group Error : $group")
//    println(s"System Error : $system")
//    (group, system)
//  }
//
//  // ----------------------- Ignore error lookup -----------------------
//  /**
//   * Reads the ignore list from a config table and returns true if we SHOULD send the message.
//   *
//   * @param ignoreTable fully qualified table name (e.g. db.schema.table)
//   */
//  def shouldSendByError(
//                         spark: SparkSession,
//                         ignoreTable: String,
//                         jobName: String,
//                         errMsg: String,
//                         errGroup: String
//                       ): Boolean = {
//    import spark.implicits._
//
//    val query = s"""
//                   |select job_nm, system_error, error_group, error_msg
//                   |from %s
//                   |where job_nm = '%s' AND flag = 'Y'
//                   |""".stripMargin.format(ignoreTable, jobName)
//
//    val df: DataFrame = spark.sql(query)
//
//    val groups = df.select("error_group").na.drop()
//      .as[String].map(_.toLowerCase).filter(s => s.nonEmpty && s != "null" && s != "none").collect().toSeq
//
//    val msgs = df.select("error_msg").na.drop()
//      .as[String].map(_.toLowerCase).filter(s => s.nonEmpty && s != "null" && s != "none").collect().toSeq
//
//    println(s"List ignore error group $groups")
//    println(s"List ignore error detail $msgs")
//
//    val eGroup = Option(errGroup).getOrElse("").toLowerCase
//    val eMsg = Option(errMsg).getOrElse("").toLowerCase
//
//    val groupIgnored = eGroup.nonEmpty && groups.exists(g => eGroup.contains(g))
//    val msgIgnored = msgs.exists(m => eMsg.contains(m))
//
//    val send = !(groupIgnored || msgIgnored)
//    if (!send) println("Skip Sent Notification message in ignore error list")
//    send
//  }
//
//  // ----------------------- Core orchestrator -----------------------
//  /**
//   * Scala port of notifier_core.
//   *
//   * @param notiType    one of: line | msteam | email (case-insensitive)
//   * @param notiEnable  'Y' to enable
//   * @param delayFlag   'Y' to gate by lastDelayDay
//   * @param lastDelayDay compare against refDate (YYYY-MM-DD)
//   * @param token       for LINE (Bearer) or MSTeams (webhook URL)
//   */
//  def notifierCore(
//                    spark: SparkSession,
//                    jobName: String,
//                    status: String,
//                    refDate: String, // YYYY-MM-DD
//                    errMsg: String = "-",
//                    rawTableName: String = "",
//                    token: String = "",
//                    notiType: String = "",
//                    notiEnable: String = "",
//                    delayFlag: String = "",
//                    lastDelayDay: String = "",
//                    webHookUrl: String = "", // will be overridden by token for msteam
//                    sender: String = "",
//                    password: String = "",
//                    recipients: String = "",
//                    mode: String = "",
//                    errGroup: String = "",
//                    ignoreTable: String, // required: fully-qualified table for ignore list
//                    passthroughErrors: Set[String] = Set.empty // names of exception classes to rethrow
//                  ): Unit = {
//    try {
//      println(s"ref_date : $refDate")
//      println(s"last_delay_day : $lastDelayDay")
//
//      val enabled = Option(notiEnable).exists(_.equalsIgnoreCase("y"))
//      val delayEnabled = Option(delayFlag).exists(_.equalsIgnoreCase("y"))
//
//      var shouldSend = true
//
//      if (!enabled) {
//        shouldSend = false
//        println("Skip Sent Notification Flag is not Y")
//      }
//
//      if (enabled && delayEnabled && Option(refDate).exists(r => r != lastDelayDay)) {
//        shouldSend = false
//        println("Skip Sent Notification is not last delay date")
//      }
//
//      if (enabled && shouldSend) {
//        val sendByMsg = shouldSendByError(
//          spark,
//          ignoreTable,
//          jobName,
//          Option(errMsg).getOrElse(""),
//          Option(errGroup).getOrElse("")
//        )
//        shouldSend = sendByMsg
//        if (!sendByMsg) println("This Notification is Ignored By Ignore Error List")
//      }
//
//      if (!shouldSend) return
//
//      notiType.toLowerCase match {
//        case "line"   => lineNotify(jobName, status, errMsg, rawTableName, token, if (mode.nonEmpty) mode else "Transform")
//        case "msteam" => msteamNotify(jobName, rawTableName, if (mode.nonEmpty) mode else "Transform", errMsg, status, if (webHookUrl.nonEmpty) webHookUrl else token)
//        case "email"  => emailNotify(jobName, rawTableName, if (mode.nonEmpty) mode else "Transform", errMsg, status, sender, password, recipients)
//        case other     => println(s"Noti Type Can't Support: $other")
//      }
//
//      println("Notification Complete")
//
//    } catch {
//      case e: Throwable =>
//        val name = e.getClass.getSimpleName
//        if (passthroughErrors.contains(name)) throw e
//        else throw SentNotificationError(e)
//    }
//  }
//}
