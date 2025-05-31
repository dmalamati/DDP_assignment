import org.apache.spark.sql.SparkSession
import scala.util.Random

object OptimalGroupAllPairs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Optimal Group All-Pairs Matching")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val numOfRecords = 3000 // The wanted amount of data records (d)
    val recordLength = 100 // The wanted record size in bytes

    //////////////////////////// Create Data /////////////////////////////

    // Create synthetic data record: (i, record_i)
    val syntheticData = (1 to numOfRecords).map { i =>
      val record = Random.alphanumeric.take(recordLength).mkString // create a random string of 100 characters as an item record (~200bytes) -> .take(1024 * 1024) for 1MB per record
      (i, record)
    }

    // Turn the synthetic data into an RDD
    val recordRDD = sc.parallelize(syntheticData)

    ////////////////////// Find Optimal p Function ///////////////////////

    def isPrime(n: Int): Boolean = {
      if (n < 2) false
      else !(2 to math.sqrt(n).toInt).exists(n % _ == 0)
    }

    def findOptimalPrime(d: Int): Option[Int] = {
      val max_p = math.sqrt(d).toInt
      (2 to max_p).reverse.find{ p =>
        isPrime(p) && d % (p * p) == 0
      }
    }

    val maybeP = findOptimalPrime(numOfRecords)

    if (maybeP.isEmpty) {
      println(s"No valid prime p found such that p^2 divides $numOfRecords.")
      System.exit(1)
    }

    val p = maybeP.get
    val q = numOfRecords / p // reducer size
    val r = p + 1            // replication rate
    val g = p * p            // number of reducers

    println(s"Optimal parameters for $numOfRecords data records(d): prime number(p)=$p, reducer size(q)=$q, replication rate(r)=$r, number of reducers/groups(g)=$g")

    ////////////////// Optimal Group All-Pairs Approach //////////////////

    val startTime = System.nanoTime()

    // Map phase: generate ((row, col), (i, Ri)) key-value pairs where (row, col) the array 'coordinates' that correspond to a reducer
    val mappedOptimalGroupPairs = recordRDD.flatMap { case (i, record_i) =>
      val row = i / p // find the corresponding row for record_i
      val col = i % p // find the corresponding column for record_i

      // Find the p column reducers and 1 diagonal reducer that record_i must be assigned to
      val reducers = (0 until p).map(j => (row, j)) :+ (col, col)

      // Use the reducer id as the key of the new pair
      reducers.map { reducer => (reducer, (i, record_i)) }
    }

    // Reduce phase: receives ((row, col), List[records from g(i)]) key-value pairs where (row, col) the array 'coordinates' that correspond to a reducer
    val reducedOptimalGroupPairs = mappedOptimalGroupPairs.mapValues(v => List(v)).reduceByKey(_ ++ _)


    val endTime = System.nanoTime()
    val executionTimeMs = (endTime - startTime) / 1e6 // Convert to milliseconds

    // Force mappedOptimalGroupPairs and reducedOptimalGroupPairs RDD execution
    println("\nNumber of pairs after map phase: " + mappedOptimalGroupPairs.count())
    println("\nNumber of pairs after reduce phase: " + reducedOptimalGroupPairs.count())

    ////////////////////////////// Outputs ///////////////////////////////

    val replicationRate = r
    val numPairs = mappedOptimalGroupPairs.count()
    val sizePerPair = 4 + recordLength  // roughly 4 bytes for the reducer id + record size in bytes
    val totalBytes = numPairs * sizePerPair
    val totalMB = totalBytes / (1024.0 * 1024.0)

    println("\nReplication Rate: " + replicationRate)
    println("Communication Cost in pairs: " + numPairs)
    println("Estimated communication cost in MB: " + totalMB)
    println("Execution Time (ms): " + executionTimeMs)

    // Reference for the shape of the pairs after map phase
    println("\nSample of pairs after map phase: " )
    mappedOptimalGroupPairs.take(5).foreach(println)

    // Reference for the shape of the pairs after reduce phase
    println("\nSample of pairs after reduce phase: ")
    reducedOptimalGroupPairs.take(5).foreach(println)

    spark.stop()

  }
}
