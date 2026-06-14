import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Question4 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Question4")
      .getOrCreate()

    // Διαδρομή Αρχείου στο HDFS
    val hdfsPath = "hdfs://localhost:9000/data/country_vaccinations_by_manufacturer.csv"

    // Φόρτωση Δεδομένων
    val initialDF = spark.read
      .option("header", "true")
      .option("sep", ";")
      .option("inferSchema", "true")
      .csv(hdfsPath)
      .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
      .filter(col("date").isNotNull)


    // ΕΡΩΤΗΜΑ 1
    //  Υπολογίζουμε τον μέσο όρο και τον φέρνουμε ως Array με το collect()
    val avgDF = initialDF.agg(avg("total_vaccinations"))
    // Παίρνουμε τη γραμμή 0 και τη στήλη 0, το κάνουμε String και μετά Double
    val avgAll = avgDF.head().getDouble(0)

    println("Γενικός μέσος όρος : " + avgAll.toLong) // Χρήση toLong για εμφάνιση ως ακέραιος

    println("\n>>> AVG per year")
    initialDF
      .withColumn("year", year(col("date")))
      .groupBy("year")
      .agg(avg("total_vaccinations").cast("long").as("Average_Vaccinations")) // Cast σε long για απλότητα
      .orderBy("year")
      .show()

    //  ΕΡΩΤΗΜΑ 2
    val resultQ2 = initialDF
      .filter(col("total_vaccinations") > avgAll)
      .groupBy("location")
      .agg(count("*").as("Days_Above_Global_Avg"))
      .orderBy(col("Days_Above_Global_Avg").desc)

    println("\n Τοποθεσίες με ημέρες πάνω από το μέσο όρο (" + avgAll.toLong + "):")
    resultQ2.show(20)

    resultQ2.write.mode("overwrite").option("header", "true").csv("output/exercise4_q2")

    // ΕΡΩΤΗΜΑ 3: Δημοφιλέστερο και λιγότερο δημοφιλές εμβόλιo
    println("\n Δημοφιλέστερο και λιγότερο δημοφιλές εμβόλιο ανά χώρα")

    // Βρίσκουμε τα συνολικά εμβόλια ανά χώρα και τύπο
    val vaxSums = initialDF.groupBy("location", "vaccine")
      .agg(sum("total_vaccinations").as("total"))

    // Βρίσκουμε το MAX και MIN νούμερο ανά χώρα
    val maxPerCountry = vaxSums.groupBy("location").max("total")
    val minPerCountry = vaxSums.groupBy("location").min("total")

    // Κάνουμε Join για να βρούμε ποιο εμβόλιο έχει το MAX νούμερο
    val maxVac = maxPerCountry.as("m").join(vaxSums.as("v"),
      col("m.location") === col("v.location") &&
        col("m.max(total)") === col("v.total")
    ).select(col("m.location").as("Country"), col("v.vaccine").as("Most_Used_Vaccine"))

    // Κάνουμε Join για να βρούμε ποιο εμβόλιο έχει το MIN νούμερο
    val minVac = minPerCountry.as("min").join(vaxSums.as("v"),
      col("min.location") === col("v.location") &&
        col("min.min(total)") === col("v.total")
    ).select(col("min.location").as("Country"), col("v.vaccine").as("Least_Used_Vaccine"))

    // Ενώνουμε τα δύο τελικά αποτελέσματα για να τα τυπώσουμε μαζί
    val finalDF = maxVac.join(minVac, "Country").orderBy("Country")

    finalDF.show(20)

    finalDF.write.mode("overwrite").option("header", "true").csv("output/exercise4_q3")

    spark.stop()
  }
}