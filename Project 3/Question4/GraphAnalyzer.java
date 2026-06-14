import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * ΥΠΟΕΡΓΑΣΙΑ 4 - ΥΠΟΕΡΩΤΗΜΑ 1
 * Υπολογισμός του βαθμού (degree) κάθε κορυφής ενός γράφου.
 * Εφαρμόζεται στατικό φιλτράρισμα ακμών βάσει παραμέτρου κατωφλίου T.
 */
public class GraphAnalyzer {

    /**
     * Mapper: Αναλαμβάνει την ανάγνωση των ακμών και το φιλτράρισμα βάσει πιθανότητας.
     */
    public static class DegreeMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text node = new Text();
        private DoubleWritable prob = new DoubleWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Ανάκτηση της τιμής T από τις ρυθμίσεις του Job (Configuration)
            double thresholdT = context.getConfiguration().getDouble("THRESHOLD_T", 0.0);

            // Διαχωρισμός γραμμής: αναμένεται μορφή "Κόμβος1 Κόμβος2 Πιθανότητα"
            String[] parts = value.toString().split(" ");
            if (parts.length == 3) {
                double p = Double.parseDouble(parts[2]);

                /**
                 * ΦΙΛΤΡΑΡΙΣΜΑ: Αγνοούμε όλες τις ακμές
                 * με πιθανότητα μικρότερη από την τιμή T.
                 */
                if (p >= thresholdT) {
                    prob.set(p);

                    /**
                     * Επειδή ο βαθμός μιας κορυφής είναι το άθροισμα των πιθανοτήτων 
                     * όλων των ακμών που προσπίπτουν σε αυτήν, καταχωρούμε την πιθανότητα
                     * και για τους δύο κόμβους που ορίζουν την ακμή.
                     */
                    node.set(parts[0]);
                    context.write(node, prob); // Συσχέτιση πιθανότητας με τον 1ο κόμβο

                    node.set(parts[1]);
                    context.write(node, prob); // Συσχέτιση πιθανότητας με τον 2ο κόμβο
                }
            }
        }
    }

    /**
     * Reducer: Υπολογίζει τον τελικό βαθμό κάθε κορυφής.
     */
    public static class DegreeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            // Άθροιση των πιθανοτήτων για όλες τις ακμές που συνδέονται με τον συγκεκριμένο κόμβο
            for (DoubleWritable val : values) {
                sum += val.get();
            }

            // Εγγραφή του τελικού αποτελέσματος: (Όνομα_Κόμβου, Συνολικός_Βαθμός)
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        /**
         * Λήψη της παραμέτρου T από τη γραμμή εντολών.
         * Η τιμή αυτή περνάει στο Configuration για να είναι προσβάσιμη από τους Mappers.
         */
        if (args.length > 2) {
            conf.setDouble("THRESHOLD_T", Double.parseDouble(args[2]));
        }

        Job job = Job.getInstance(conf, "Graph Degree Calculation");
        job.setJarByClass(AdvanceGraphAnalyzer.class);

        // Ορισμός κλάσεων Mapper και Reducer
        job.setMapperClass(DegreeMapper.class);
        job.setReducerClass(DegreeReducer.class);

        // Ορισμός τύπων δεδομένων εξόδου
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Ορισμός διαδρομών αρχείου εισόδου και φακέλου εξόδου
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Εκτέλεση της εργασίας και έξοδος
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}