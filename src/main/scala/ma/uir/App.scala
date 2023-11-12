
/**

package ma.uir
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.spark.sql.{Row, SparkSession}

 /** @author ${user.name}**/
object App {



  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark HBase Writer")
      .getOrCreate()

    // Sample DataFrame with Rows. Replace this with your actual data.
    val data: Seq[Row] = Seq(
      Row("row1", "columnFamily1", "columnQualifier1", "value1"),
      Row("row2", "columnFamily2", "columnQualifier2", "value2")
      // Add more rows as needed
    )

    // Configuration for HBase
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "localhost") // Set the Zookeeper quorum

    val connection: Connection = ConnectionFactory.createConnection(hbaseConf)
    val table: Table = connection.getTable(TableName.valueOf("oussama_Bucket_List")) // Replace with your table name

    // Write the Rows to HBase
    data.foreach { row =>
      val put = new Put(row.getString(0).getBytes)
      put.addColumn(row.getString(1).getBytes, row.getString(2).getBytes, row.getString(3).getBytes)
      table.put(put)
    }

    // Close the HBase resources
    table.close()
    connection.close()

    spark.stop()
  }
}

**/
package ma.uir
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructType, StructField, StringType}

import java.util.{ArrayList, List}

object SparkHBaseWriter {
  def main(args: Array[String]): Unit = {
    // Configuration Spark
    val spark = SparkSession.builder
      .appName("SparkHBaseWriter")
      .master("yarn") //
      .getOrCreate()

    // Création d'un DataFrame avec les données à insérer
    val data = Seq(
      ("101", "John White", "Los Angeles, CA", "Chairs", "$400.00"),
      ("102", "Jane Brown", "Atlanta, GA", "Lamps", "$200.00"),
      ("103", "Bill Green", "Pittsburgh, PA", "Desk", "$500.00"),
      ("104", "Jack Black", "St. Louis, MO", "Bed", "$1,600.00")
    )

    val columns = Seq("ID", "Name", "City", "Product", "Amount")

    val javaList: List[Row] = new ArrayList[Row]()
    data.foreach { tuple =>
      javaList.add(Row.fromTuple(tuple))
    }

    val schema = StructType(columns.map(fieldName => StructField(fieldName, StringType, nullable = true)))

    val df = spark.createDataFrame(javaList, schema)

    // Show DataFrame
    df.show()

    // Perform further processing or write to HBase as needed

    // Stop Spark session
    spark.stop()
  }
}

