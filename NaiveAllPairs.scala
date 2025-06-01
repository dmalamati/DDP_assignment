import org.apache.spark.sql.SparkSession
import scala.util.Random

object NaiveAllPairs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Naive All-Pairs Matching")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // val numOfRecords = 3000 // The wanted amount of data records
    // val recordLength = 100 // The wanted record size in bytes

    val recordLength = if (args.length > 0) args(0).toInt else 100 // The wanted record size in bytes
    val numOfRecords = if (args.length > 1) args(1).toInt else 3000 // The wanted amount of data records
    val numOfGroups = numOfRecords // Naive ≡ Each record as its own group

    println(s"Inputs: record length=$recordLength, number of records=$numOfRecords")

    //////////////////////////// Create Data /////////////////////////////

    // Create synthetic data record: (i, record_i)
    val syntheticData = (1 to numOfRecords).map { i =>
      // Create a random string of length=recordLength, which roughly translates to recordLength*1 bytes when using UTF-8 format
      val record = Random.alphanumeric.take(recordLength).mkString
      (i, record)
    }

    // Turn the synthetic data into an RDD
    val recordRDD = sc.parallelize(syntheticData)

    ////////////////////// Naive All-Pairs Approach //////////////////////

    val startTime = System.nanoTime()


    // Map phase: generate ({i, j}, Ri) key-value pairs where i≠j
    val mappedPairs = recordRDD.flatMap { case (i, record_i) =>
      (1 to numOfRecords).filter(_ != i).map { j =>
        val key = if (i < j) (i, j) else (j, i)
        (key, record_i)
      }
    }

    // Reduce phase: receives ({i, j}, [Ri, Rj]) key-value pairs where i≠j
    val reducedPairs = mappedPairs.mapValues(v => List(v)).reduceByKey(_ ++ _) // Create a list of records


    val endTime = System.nanoTime()
    val executionTimeMs = (endTime - startTime) / 1e6  // Convert to milliseconds

    // Force mappedPairs and reducedPairs RDD execution
    println("\nNumber of pairs after map phase: " + mappedPairs.count())
    println("\nNumber of pairs after reduce phase: " + reducedPairs.count())

    ////////////////////////////// Outputs ///////////////////////////////

    val replicationRate = numOfGroups - 1 // numOfGroups = numOfRecords
    val numPairs = mappedPairs.count()
    val sizePerPair = 8 + recordLength  // roughly 8 bytes for (i, j) + record size in bytes
    val totalBytes = numPairs * sizePerPair
    val totalMB = totalBytes / (1024.0 * 1024.0)

    println("\nReplication Rate: " + replicationRate)
    println("Communication Cost in pairs: " + numPairs)
    println("Estimated communication cost in MB: " + totalMB)
    println("Execution Time (ms): " + executionTimeMs)

    // Reference for the shape of the pairs after map phase
    println("\nSample of pairs after map phase: " )
    mappedPairs.take(5).foreach(println)

    // Reference for the shape of the pairs after reduce phase
    println("\nSample of pairs after reduce phase: ")
    reducedPairs.take(5).foreach(println)

    spark.stop()

  }
}
