import flexiblesql.EvolvingSchemaSqlWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit


object Main {
  val user = "yoursqllogin"
  val password = "yoursqlpassword"
  val sqlServer = "yourservername.database.windows.net"
  val sqlDbName = "yourdbname"
  val conn = s"jdbc:sqlserver://$sqlServer:1433;database=$sqlDbName;encrypt=true;trustServerCertificate=false;" +
    "hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]").appName("EvolvingSqlSchema")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val tableName = "newtable"
    val columns = Seq("id", "name")
    val data = Seq(("1", "Jakub"), ("2", "Jake"), ("3", "James"))
    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd).toDF(columns: _*)

    val evolvingSchemaSqlWriter = new EvolvingSchemaSqlWriter(conn, user, password)
    evolvingSchemaSqlWriter.writeDfToSql(df, tableName)
    evolvingSchemaSqlWriter.writeDfToSql(df.withColumn("Surname", lit("Smith")), tableName)
  }
}
