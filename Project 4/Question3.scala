import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Question3 {

  def main(args: Array[String]): Unit = {

    // Δημιουργία Spark Session
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Question3")
      .getOrCreate()

    val hdfsPath = "hdfs://localhost:9000/data/movies.csv"

    // Φόρτωση Δεδομένων
    val moviesDF = spark.read
      .option("header", "true")
      .option("sep", ";")
      .option("inferSchema", "true")
      .csv(hdfsPath)

    // Καθαρισμός
    val cleanDF = moviesDF
      .withColumn("imdbRating", col("imdbRating").cast("double")) // <-- Η ΔΙΟΡΘΩΣΗ ΕΔΩ
      .select("imdbID", "title", "year", "imdbRating", "country")
      .na.drop()

    // ΕΡΩΤΗΜΑ 1: Μέσο σκορ και πλήθος ταινιών ανά έτος
    println("\n ΣΤΑΤΙΣΤΙΚΑ ΑΝΑ ΕΤΟΣ")
    cleanDF.groupBy("year")
      .agg(
        round(avg("imdbRating"),2),
      )
      .orderBy(desc("year"))
      .show(10)


    // ΕΡΩΤΗΜΑ 2: Κορυφαία ταινία ανά χώρα
    println("\nΚΑΛΥΤΕΡΗ ΤΑΙΝΙΑ ΑΝΑ ΧΩΡΑ : ")

    // Διαχωρισμός των χωρών
    val tempDF = cleanDF
      .withColumn("single_country", explode(split(col("country"), ","))) // Κόβει και δημιουργεί νέες γραμμές
      .withColumn("single_country", trim(col("single_country")))

    // Βρίσκουμε το Max με βάση τη νέα στήλη
    val maxRatingsPerCountry = tempDF.groupBy("single_country").max("imdbRating")

    // Join
    val bestMovies = maxRatingsPerCountry.as("df1").join(tempDF.as("df2"),
      col("df1.single_country") === col("df2.single_country") &&
        col("df1.max(imdbRating)") === col("df2.imdbRating")
    )
      .select(
        col("df2.single_country").as("Country"), // Το ονομάζουμε Country για την εκτύπωση
        col("df2.title").as("Title"),
        col("df2.imdbRating").as("Rating")
      )
      .orderBy("Country")

    bestMovies.show(20)

    //  ΕΡΩΤΗΜΑ 3: Ζεύγη ταινιών με διαφορά σκορ <= 1
    println("\nΖΕΥΓΗ ΤΑΙΝΙΩΝ ΜΕ ΔΙΑΦΟΡΑ <= 1")


    val partOfDF = cleanDF.limit(100)

    val pairs = partOfDF.as("df1").join(partOfDF.as("df2"))
      .where("df1.imdbID < df2.imdbID") // Για να μην έχουμε διπλά ζευγάρια
      .where(abs(col("df1.imdbRating") - col("df2.imdbRating")) <= 1.0)
      .select(
        col("df1.title").as("Movie1"),
        col("df2.title").as("Movie2"),
        col("df1.imdbRating"),
        col("df2.imdbRating")
      )

    pairs.show(10)

    spark.stop()
  }
}