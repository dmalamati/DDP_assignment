import org.apache.spark.sql.SparkSession
import scala.util.Random



object GroupAllPairs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Group All-Pairs Matching")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val numOfRecords = 3000 // The wanted amount of data records
    val numOfGroups = 30 // The wanted amount of groups

    //////////////////////////// Create Data /////////////////////////////

    // Create synthetic data record: (i, record_i)
    val syntheticData = (1 to numOfRecords).map { i =>
      val record = Random.alphanumeric.take(100).mkString // create a random string of 100 characters as an item record (~200bytes) -> .take(1024 * 1024) for 1MB per record
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

    // Print head of mappedGroupPairs and reducedGroupPairs for execution and reference
    println("\nNumber of all pairs: " + mappedGroupPairs.count())
    mappedGroupPairs.take(5).foreach(println)

    println("\nNumber of all pairs: " + reducedGroupPairs.count())
    reducedGroupPairs.take(5).foreach(println)

    val replicationRate = numOfGroups - 1
    val communicationCost = mappedGroupPairs.count()
    println("\nReplication Rate: " + replicationRate)
    println("\nCommunication Cost: " + communicationCost)
    println("\nExecution Time (ms): " + executionTimeMs)
  }
}