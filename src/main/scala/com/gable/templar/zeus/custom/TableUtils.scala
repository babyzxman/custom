package com.gable.templar.zeus.custom

object TableUtils {

  def tableSchemaCheck(tableName: String, tableSchema: String = ""): String = {
    val normalizedSchema = Option(tableSchema)
      .map(_.trim)
      .getOrElse("")
      .toLowerCase

    val hasSchema = normalizedSchema.nonEmpty &&
      normalizedSchema != "none" &&
      normalizedSchema != "null"

    if (hasSchema)
      s"$normalizedSchema.$tableName"
    else
      tableName
  }

}
