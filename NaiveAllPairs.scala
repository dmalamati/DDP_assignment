import org.apache.spark.sql.SparkSession
import scala.util.Random

object NaiveAllPairs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Naive All-Pairs Matching")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val numOfRecords = 3000 // The wanted amount of data records

    // Create synthetic data record: (item_i, record_i)
    val syntheticData = (1 to numOfRecords).map { i =>
      val record = Random.alphanumeric.take(100).mkString // create a random string of 100 characters as an item record (~200bytes) -> .take(1024 * 1024) for 1MB per record
      (i, record)
    }

    // Turn the synthetic data into an RDD
    val recordRDD = sc.parallelize(syntheticData)

    //    println("Length of RDD: " + recordRDD.count())
    //    recordRDD.take(5).foreach(println)

    ////////////////////// Naive All-Pairs Approach //////////////////////

    // Map phase: generate ({i, j}, Ri) key-value pairs where i≠j
    val mappedPairs = recordRDD.flatMap { case (i, record_i) =>
      (1 to numOfRecords).filter(_ != i).map { j =>
        val key = if (i < j) (i, j) else (j, i)
        (key, (i, record_i))
      }
    }

    println("\nNumber of all pairs: " + mappedPairs.count())
    mappedPairs.take(5).foreach(println)

    // Reduce phase: receives ({i, j}, [Ri, Rj]) key-value pairs where i≠j
    val reducedPairs = mappedPairs.groupByKey()

    println("\nNumber of all pairs: " + reducedPairs.count())
    reducedPairs.take(5).foreach(println)

    spark.stop()

  }

}
