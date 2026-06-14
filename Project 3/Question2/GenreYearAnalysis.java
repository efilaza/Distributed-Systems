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
import java.util.ArrayList;
import java.util.List;

/**
 * ΥΠΟΕΡΓΑΣΙΑ 2 - ΕΡΩΤΗΜΑ Β
 * Αναζήτηση και καταμέτρηση κορυφαίων ταινιών (score > 8.0) ανά έτος και ανά είδος.
 * Χρήση χειροκίνητου Parser για απόλυτη ακρίβεια στον διαχωρισμό των στηλών.
 */
public class GenreYearAnalysis {

    /**
     * Χειροκίνητος Parser που διαχειρίζεται σωστά τα κόμματα εντός εισαγωγικών.
     * Εξασφαλίζει ότι πεδία όπως τα "Genres" δεν θα "σπάσουν" σε λάθος σημεία.
     */
    private static String[] parseCSVLine(String row) {
        List<String> parts = new ArrayList<>();
        StringBuilder currentPart = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < row.length(); i++) {
            char c = row.charAt(i);

            if (c == '\"') {
                inQuotes = !inQuotes; // Εναλλαγή κατάστασης εντός/εκτός εισαγωγικών
            } else if (c == ',' && !inQuotes) {
                // Διαχωρισμός στήλης μόνο αν το κόμμα είναι εκτός εισαγωγικών
                parts.add(currentPart.toString().trim());
                currentPart.setLength(0);
            } else {
                currentPart.append(c);
            }
        }
        parts.add(currentPart.toString().trim());
        return parts.toArray(new String[0]);
    }

    public static class GenreMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text compositeKey = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            
            // Παράλειψη της κεφαλίδας
            if (line.startsWith("imdbID")) return; 

            // Κλήση του χειροκίνητου Parser
            String[] cols = parseCSVLine(line);

            if (cols.length >= 9) {
                try {
                    // Στήλη 6: score
                    double score = Double.parseDouble(cols[6].trim());
                    
                    // Φιλτράρισμα: Μόνο ταινίες με βαθμολογία > 8.0
                    if (score > 8.0) {
                        // Στήλη 2: year
                        String year = cols[2].trim();
                        // Στήλη 4: genres (αφαιρούμε τυχόν εναπομείναντα εισαγωγικά)
                        String genresRaw = cols[4].replace("\"", "").trim();

                        if (genresRaw.isEmpty()) genresRaw = "NO_GENRE";
                        
                        // Διαχωρισμός πολλαπλών ειδών (π.χ. "Action, Drama")
                        String[] genreList = genresRaw.split(",");

                        for (String g : genreList) {
                            /**
                             * Σύνθετο Κλειδί: "Έτος_Είδος"
                             * Επιτρέπει την ομαδοποίηση των δεδομένων και ανά έτος και ανά κατηγορία.
                             */
                            compositeKey.set(year + "_" + g.trim());
                            context.write(compositeKey, one); 
                        }
                    }
                } catch (NumberFormatException e) {
                    // Παράλειψη γραμμών με λάθος αριθμητική μορφή
                }
            }
        }
    }

    public static class GenreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable totalCount = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            // Άθροιση όλων των εμφανίσεων για το συγκεκριμένο Έτος και Είδος
            for (IntWritable val : values) {
                sum += val.get();
            }
            totalCount.set(sum);
            context.write(key, totalCount);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "IMDB Genre Year Analysis Manual");
        
        job.setJarByClass(GenreYearAnalysis.class);
        job.setMapperClass(GenreMapper.class);// Ορισμός Mapper
        job.setReducerClass(GenreReducer.class); // Ορισμός Reducer

		// Ορισμός τύπων δεδομένων εξόδου
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

		// Ορισμός διαδρομών εισόδου/εξόδου από τα ορίσματα της γραμμής εντολών
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}