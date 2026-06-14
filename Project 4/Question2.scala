import org.apache.spark.sql.SparkSession

object Question2 {

  def main(args: Array[String]): Unit = {

    val S = 100

    // Δημιουργία Spark Session
    val spark = SparkSession.builder()
      .appName("Question2")
      .master("local[*]")
      .getOrCreate()


    val hdfsPath = "hdfs://localhost:9000/data/groceries.csv"

    //  Διάβασμα του αρχείου
    val lines = spark.sparkContext.textFile(hdfsPath)

    //  Επεξεργασία κάθε γραμμής για εύρεση ζευγών και τριάδων
    val allCombinations = lines.flatMap(line => {
      val items = line.split(",").map(i => i.trim())

      // Ταξινομούμε τα προϊόντα αλφαβητικά για να μην έχουμε διπλά (π.χ. Milk,Bread και Bread,Milk)
      val sortedItems = items.sorted

      // Δημιουργούμε μια λίστα για να βάλουμε μέσα τα αποτελέσματα
      var tempResults = List[Any]()

      // Εύρεση Δυάδων
      for (i <- 0 until sortedItems.length) {
        for (j <- i + 1 until sortedItems.length) {
          val pair = (sortedItems(i), sortedItems(j))
          tempResults = tempResults :+ pair
        }
      }

      // Εύρεση Τριάδων
      for (i <- 0 until sortedItems.length) {
        for (j <- i + 1 until sortedItems.length) {
          for (k <- j + 1 until sortedItems.length) {
            val triplet = (sortedItems(i), sortedItems(j), sortedItems(k))
            tempResults = tempResults :+ triplet
          }
        }
      }

      tempResults
    })

    // Μέτρημα εμφανίσεων
    val frequencies = allCombinations
      .map(item => (item, 1))
      .reduceByKey((a, b) => a + b)

    //  Φιλτράρισμα με βάση το όριο S
    val frequentItems = frequencies.filter(x => x._2 >= S)

    // Ταξινόμηση (φθίνουσα)
    val sortedResults = frequentItems.sortBy(x => x._2, false)

    // Εκτύπωση των 10 πρώτων
    println("The common pairs are:")
    val finalTop10 = sortedResults.take(10)

    for (res <- finalTop10) {
      val items = res._1
      val freq= res._2
      println(items + ": " + freq)
    }

    spark.stop()
  }
}