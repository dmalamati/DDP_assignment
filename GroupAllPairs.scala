import org.apache.spark.sql.SparkSession
import scala.util.Random

object GroupAllPairs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Group All-Pairs Matching")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // val numOfRecords = 3000 // The wanted amount of data records
    // val numOfGroups = 30 // The wanted amount of groups
    // val recordLength = 100 // The wanted record size in bytes

    val recordLength = if (args.length > 0) args(0).toInt else 100 // The wanted record size in bytes
    val numOfRecords = if (args.length > 1) args(1).toInt else 3000 // The wanted amount of data records
    val numOfGroups =  if (args.length > 2) args(2).toInt else 30 // The wanted amount of groups

    println(s"Inputs: record length=$recordLength, number of records=$numOfRecords, number of groups=$numOfGroups")

    //////////////////////////// Create Data /////////////////////////////

    // Create synthetic data record: (i, record_i)
    val syntheticData = (1 to numOfRecords).map { i =>
      // Create a random string of length=recordLength, which roughly translates to recordLength*1 bytes when using UTF-8 format
      val record = Random.alphanumeric.take(recordLength).mkString 
      (i, record)
    }

    // Turn the synthetic data into an RDD
    val recordRDD = sc.parallelize(syntheticData)

    ///////////////////////// Group Function /////////////////////////

    // Function that given the index i of a record, the total number of groups and the total number of records, returns the index of the group that the record i belongs to
    def groupNumber(i: Int, numOfGroups: Int,  numOfRecords: Int) : Int = {
      val recordsPerGroup = math.ceil(numOfRecords.toDouble / numOfGroups).toInt
      // println(s"Records per group $recordsPerGroup")
      return ((i - 1) / recordsPerGroup) + 1
    }

    ////////////////////// Group All-Pairs Approach //////////////////////

    val startTime = System.nanoTime()


    // Map phase: generate ({g(i), g(j)}, (i, Ri)) key-value pairs where g(i)≠g(j)
    val mappedGroupPairs = recordRDD.flatMap { case (i, record_i) =>
      val gi = groupNumber(i, numOfGroups, numOfRecords)
      (1 to numOfGroups).filter(_ != gi).map { gj =>
        val key = if (gi < gj) (gi, gj) else (gj, gi)
        (key, (i, record_i))
      }
    }

    // Reduce phase: receives ({g(i), g(j)}, List[records from g(i) and g(j)]) key-value pairs where g(i)≠g(j)
    val reducedGroupPairs = mappedGroupPairs.mapValues(v => List(v)).reduceByKey(_ ++ _) // Create a list of all records


    val endTime = System.nanoTime()
    val executionTimeMs = (endTime - startTime) / 1e6  // Convert to milliseconds

    // Force mappedGroupPairs and reducedGroupPairs RDD execution
    println("\nNumber of pairs after map phase: " + mappedGroupPairs.count())
    println("\nNumber of pairs after reduce phase: " + reducedGroupPairs.count())

    ////////////////////////////// Outputs ///////////////////////////////

    val replicationRate = numOfGroups - 1
    val numPairs = mappedGroupPairs.count()
    val sizePerPair = 12 + recordLength  // roughly 12 bytes for (gi, gj, i) + record size in bytes
    val totalBytes = numPairs * sizePerPair
    val totalMB = totalBytes / (1024.0 * 1024.0)

    println("\nReplication Rate: " + replicationRate)
    println("Communication Cost in pairs: " + numPairs)
    println("Estimated communication cost in MB: " + totalMB)
    println("Execution Time (ms): " + executionTimeMs)

    // Reference for the shape of the pairs after map phase
    println("\nSample of pairs after map phase: " )
    mappedGroupPairs.take(5).foreach(println)

    // Reference for the shape of the pairs after reduce phase
    println("\nSample of pairs after reduce phase: ")
    reducedGroupPairs.take(5).foreach(println)

    spark.stop()

  }
}
