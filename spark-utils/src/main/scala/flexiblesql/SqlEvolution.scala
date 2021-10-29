package flexiblesql

import java.sql.{Connection, DriverManager}
import java.util.Properties
import scala.annotation.tailrec
import scala.collection.JavaConverters._

class SqlEvolution(val conn: String, val user: String, val password: String) {
  private def getDbConnection: Connection = {
    val properties = new Properties()
    properties.putAll(
      Map("url" -> conn,
        "user" -> user,
        "password" -> password).asJava)
    val connection = DriverManager.getConnection(properties.getProperty("url"), properties)
    connection
  }

  @tailrec
  private def getColumnNamesAsList(iter: List[String], resultSet: java.sql.ResultSet): List[String] = {
    if (resultSet.next()) {
      val column = resultSet.getString("COLUMN_NAME").toLowerCase
      getColumnNamesAsList(column :: iter, resultSet)
    } else iter
  }

  private def getDbColumnNames(tableName: String): List[String] = {
    val query = s"""SELECT DISTINCT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '$tableName';"""

    val readStatement = getDbConnection.prepareStatement(query)
    val resultSet = readStatement.executeQuery()
    getColumnNamesAsList(List[String](), resultSet)
  }

  def addMissingDbColumn(newColumns: Array[String], tableName: String): Unit = {
    val currentDbColumns = getDbColumnNames(tableName)
    for (columnName <- newColumns) {
      if (!currentDbColumns.contains(columnName.toLowerCase)) {
        // todo: in the future support more types by creating a mapper of types
        val alterTableStatement = getDbConnection.prepareStatement(
          s"ALTER TABLE dbo.[$tableName] ADD $columnName VARCHAR(1024);")
        alterTableStatement.executeUpdate()
      }
    }
  }
}
