package ma.uir
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{Row, SparkSession}

object SparkHBaseWriter {

  def main(args: Array[String]): Unit = {
    // Configuration Spark
    val spark = SparkSession.builder
      .appName("SparkHBaseWriter")
      .master("yarn")
      .getOrCreate()

    // Configuration HBase
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "nom_de_votre_table_hbase") // Remplacez par le nom de votre table HBase

    // Créez un RDD de test avec des Row (à remplacer par votre propre RDD)
    val data = Seq(
      Row("row1", "colFamily", "qualifier", "value1"),
      Row("row2", "colFamily", "qualifier", "value2")
    )

    // Définissez le schéma Spark DataFrame (à adapter à votre cas)
    val schema = ??? // Définissez le schéma de votre DataFrame

    // Créez le DataFrame à partir du RDD de test et du schéma
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Convertissez le DataFrame en RDD de type (ImmutableBytesWritable, Put)
    val hbaseRdd = df.rdd.map { row =>
      val put = new Put(Bytes.toBytes(row.getString(0))) // La première colonne est considérée comme la clé de ligne
      put.addColumn(Bytes.toBytes(row.getString(1)), Bytes.toBytes(row.getString(2)), Bytes.toBytes(row.getString(3)))
      (new ImmutableBytesWritable, put)
    }

    // Écrivez le RDD dans HBase
    hbaseRdd.saveAsNewAPIHadoopDataset(hbaseConf)

    // Fermez la session Spark
    spark.stop()
  }
}

