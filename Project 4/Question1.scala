import org.apache.spark.sql.SparkSession

object Question1 {

  def main(args: Array[String]): Unit = {

    // Δημιουργία Spark Session
    val spark = SparkSession.builder()
      .appName("Question1")
      .master("local[*]")
      .getOrCreate()

    // Διαδρομή αρχείου
    //val hdfsPath = "hdfs://localhost:9000/data/SherlockHolmes.txt"
    val hdfsPath = "hdfs://localhost:9000/data/Shakespeare.txt"

    //  Ανάγνωση του κειμένου
    val rawText = spark.sparkContext.textFile(hdfsPath)

    // Kαθαρισμός και Μετατροπή σε πεζά
    val lowerText = rawText.map(line => line.toLowerCase())
    val cleanText = lowerText.map(line => line.replaceAll("[^A-Za-z]+", " "))

    // 5. Δημιουργία ζευγών
    val allPairs = cleanText.flatMap(line => {
      val words = line.split(" ").filter(w => w != "")

      // Δημιουργούμε μια λίστα για να αποθηκεύσουμε τα ζεύγη της γραμμής
      if (words.length >= 2) {
        for (i <- 0 until words.length - 1) yield {
          (words(i), words(i+1))
        }
      } else {
        List()
      }
    })

    // ΕΡΩΤΗΜΑ 1 - Καταμέτρηση
    val allPairsCounts = allPairs
      .map(pair => (pair, 1))
      .reduceByKey((a, b) => a + b)

    println("ΕΡΩΤΗΜΑ 1 - Συνολικά διαφορετικά ζεύγη που εντοπίστηκαν: " + allPairsCounts.count())

    // Φιλτράρισμα βάσει μήκους
    val filteredPairs = allPairs.filter(pair => pair._1.length >= 3 && pair._2.length >= 3)

    // ΕΡΩΤΗΜΑ 2 - Καταμέτρηση
    val filteredPairsCounts = filteredPairs
      .map(pair => (pair, 1))
      .reduceByKey((a, b) => a + b)

    println("ΕΡΩΤΗΜΑ 2 - Ζεύγη που πληρούν το κριτήριο μήκους (>= 3): " + filteredPairsCounts.count())

    // 7. Ταξινόμηση και λήψη των 5 πρώτων (ΕΡΩΤΗΜΑ 3)
    // Χρήση false για φθίνουσα σειράSS
    val top5 = filteredPairsCounts.sortBy(item => item._2, false).take(5)

    // 8. Εκτύπωση αποτελεσμάτων με τη μορφή <λέξη, πλήθος> που ζητάει η εκφώνηση
    println("\nΕΡΩΤΗΜΑ 3 - TOP 5 MOST FREQUENT PAIRS")
    for (result <- top5) {
      val pair = result._1
      val count = result._2
      println("Pair: (" + pair._1 + ", " + pair._2 + "), " + count)
    }

    spark.stop()
  }
}