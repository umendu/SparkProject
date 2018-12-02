import org.apache.spark._
import org.apache.spark.sql.hive.HiveContext
import java.text.SimpleDateFormat
import java.util.Calendar

object MySqlConnectTest {

	def main(args: Array[String]) {
		var table_name: String = ""
				var period: String = ""

				//Checking Parameter Begin
				if (args.length == 4) {
					table_name=args(1)
							if(args(2) == 'Y' || args(2) == 'y') {
								val Partition = args(2)
										period = args(3)
							} else {
								period = "abcd"
							}      
				}else if (args.length == 3) {
					if(args(2) == 'Y' || args(2) == 'y') {
						sys.exit(0)
					}
				}else {
					println("ERROR : Please pass parameters DBName, TableName, Y/N, Period")
					sys.exit(0)
				}
		//Checking Parameters END
var connection: String = ""
var passfile: String = ""
		if(args(1) == "pware") {
			connection = "jdbc:mysql://localhost:3306/scala"
					passfile = ""
		} else if(args(1) == "qware") {
			connection = "jdbc:mysql://localhost:3306/scala"
					passfile = ""
		}else if(args(1) == "dware") {
			connection = "jdbc:mysql://localhost:3306/scala"
					passfile = ""
		}else if(args(1) == "pcds") {
			connection = "jdbc:mysql://localhost:3306/scala"
					passfile = ""
		}else if(args(1) == "qcds") {
			connection = "jdbc:mysql://localhost:3306/scala"
					passfile = ""
		} else if(args(1) == "dcds") {
			connection = "jdbc:mysql://localhost:3306/scala"
					passfile = ""
		} else {
			println("Error: First parameter sholud be DataBase Name between ware and cds")
			sys.exit(0)
		}

		//Intializing Command line arguments..
		val db_name=args(0)
				val tbl_name=args(1)
						val	part_tbl=args(2)
								var		part_prd=""
								val		hive_tgt_dir=""
								val		partition_col=""

								if(part_tbl == "Y")
								{
									println("Table "+tbl_name+" is a partition table")
									part_prd=args(3)
								}


		val conf = new SparkConf().setAppName("JDBC Test")
				val sc = new SparkContext(conf)
		val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);

		val dataframe_mysql = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/scala").
				option("driver", "com.mysql.jdbc.Driver").option("dbtable", "scala_test").option("user", "root").
				option("password", "cloudera").load()

				dataframe_mysql.saveAsTable("scala.scala_test_1")
				dataframe_mysql.sqlContext.sql("select * from scala_test").collect.foreach(println)
	}
}