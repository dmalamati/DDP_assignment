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
    val numOfGroups = numOfRecords // Naive ≡ Each record as its own group

    //////////////////////////// Create Data /////////////////////////////

    // Create synthetic data record: (i, record_i)
    val syntheticData = (1 to numOfRecords).map { i =>
      val record = Random.alphanumeric.take(100).mkString // create a random string of 100 characters as an item record (~200bytes) -> .take(1024 * 1024) for 1MB per record
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

    // Print head of mappedGroupPairs and reducedGroupPairs for execution and reference
    println("\nNumber of all pairs: " + mappedPairs.count())
    mappedPairs.take(5).foreach(println)

    println("\nNumber of all pairs: " + reducedPairs.count())
    reducedPairs.take(5).foreach(println)


    val replicationRate = numOfGroups - 1 // numOfGroups = numOfRecords
    val communicationCost = mappedPairs.count()
    println("\nReplication Rate: " + replicationRate)
    println("\nCommunication Cost: " + communicationCost)
    println("\nExecution Time (ms): " + executionTimeMs)

    spark.stop()

  }

}