import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * ΥΠΟΕΡΓΑΣΙΑ 2 - ΥΠΟΕΡΩΤΗΜΑ 1
 * Υπολογισμός συνολικής διάρκειας ταινιών ανά χώρα (Υποεργασία 2 - Ερώτημα Α).
 * Διαχειρίζεται αρχεία CSV με πεδία που περιέχουν κόμματα εντός εισαγωγικών.
 */
public class CountryRuntimeAnalysis {


/**
     * Parser για σωστό διαχωρισμό στηλών CSV.
     * Διαχειρίζεται κόμματα που βρίσκονται εντός εισαγωγικών.
     */
    private static String[] parseCSVLine(String row) {
        List<String> parts = new ArrayList<>(); 
        StringBuilder currentPart = new StringBuilder();
        boolean inQuotes = false; 

        for (int i = 0; i < row.length(); i++) {
            char c = row.charAt(i);

            if (c == '\"') {
                inQuotes = !inQuotes; // Εναλλαγή κατάστασης όταν βρούμε εισαγωγικά
            } else if (c == ',' && !inQuotes) {
                // Διαχωρισμός μόνο αν το κόμμα είναι εκτός εισαγωγικών 
                parts.add(currentPart.toString().trim());
                currentPart.setLength(0);
            } else {
                currentPart.append(c);
            }
        }
        parts.add(currentPart.toString().trim()); // Προσθήκη τελευταίας στήλης
        return parts.toArray(new String[0]);
    }

    public static class RuntimeMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Text countryKey = new Text();
        private IntWritable minutesValue = new IntWritable();

        @Override
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String row = value.toString();

            // Παράλειψη κεφαλίδας
            if (row.startsWith("imdbID")) return;

            // Κλήση συνάρτησης 
            String[] parts = parseCSVLine(row);


            // Έλεγχος εγκυρότητας (Στήλη 3: Runtime, Στήλη 8: Country)
            if (parts.length >= 9) {
                String runtimeRaw = parts[3].replace(" min", "").trim();
                String countriesRaw = parts[8].replace("\"", "").trim();

                try {
                    int duration = Integer.parseInt(runtimeRaw);
                    String[] countryList = countriesRaw.split(",");

                    for (String country : countryList) {
                        String cleanCountry = country.trim();
                        if (cleanCountry.isEmpty()) cleanCountry = "UNKNOWN";

                        countryKey.set(cleanCountry);
                        minutesValue.set(duration);
                        context.write(countryKey, minutesValue);
                    }
                } catch (NumberFormatException e) {
                    // Παράλειψη γραμμών με μη έγκυρη διάρκεια
                }
            }
        }
    }

    /**
     * Reducer: Λειτουργεί ως συσσωρευτής (accumulator).
     * Αθροίζει τις διάρκειες για κάθε χώρα.
     */
    public static class RuntimeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable totalResult = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalMinutes = 0;
            // Διαδρομή όλων των τιμών που αντιστοιχούν στο κλειδί (χώρα)
            for (IntWritable val : values) {
                totalMinutes += val.get();
            }
            totalResult.set(totalMinutes);
            // Τελική έξοδος: (Χώρα, Συνολικά Λεπτά)
            context.write(key, totalResult);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "IMDB Country Analysis");
        
        job.setJarByClass(CountryRuntimeAnalysis.class);
        job.setMapperClass(RuntimeMapper.class); // Ορισμός Mapper
        job.setReducerClass(RuntimeReducer.class); // Ορισμός Reducer

        // Ορισμός τύπων δεδομένων εξόδου
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Ορισμός διαδρομών εισόδου/εξόδου από τα ορίσματα της γραμμής εντολών
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}