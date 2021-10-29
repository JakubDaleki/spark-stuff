package flexiblesql

import org.apache.spark.sql.{AnalysisException, DataFrame}

class EvolvingSchemaSqlWriter(val conn: String, val user: String, val password: String) {
  val db = new SqlEvolution(conn, user, password)
  def writeDfToSql(df: DataFrame, tableName: String): Unit = {
    val sqlTableName = tableName.toLowerCase
    val write = () => {
      df.write
        .format("jdbc")
        .option("url", conn)
        .mode("append")
        .option("dbtable", s"[$sqlTableName]")
        .option("user", user)
        .option("password", password)
        .save()
    }

    try {
      write()
    } catch {
      case _: AnalysisException =>
        db.addMissingDbColumn(df.schema.fieldNames, sqlTableName)
        write()
    }
  }
}
