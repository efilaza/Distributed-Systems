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

/**
 * ΥΠΟΕΡΓΑΣΙΑ 3
 * Αναλυτής αλληλουχιών DNA για την καταμέτρηση n-grams (2, 3 και 4 χαρακτήρων).
 * Η υλοποίηση βασίζεται στην τεχνική του ολισθαίνοντος παραθύρου (sliding window).
 */
public class DNASequenceAnalyzer {

    /**
     * Βοηθητική μέθοδος παραγωγής n-grams.
     * Τεμαχίζει την αλυσίδα DNA σε όλα τα δυνατά υπο-τμήματα, διατηρώντας τις αλληλοεπικαλύψεις.
     */
    private static ArrayList<String> findNgrams(String sequence) {
        ArrayList<String> results = new ArrayList<>();
        int len = sequence.length();

        for (int i = 0; i < len; i++) {
			
            
            // Δημιουργία 2-gram (π.χ. AT)
            if (i + 2 <= len) results.add(sequence.substring(i, i + 2));
            
            // Δημιουργία 3-gram (π.χ. ATG)
            if (i + 3 <= len) results.add(sequence.substring(i, i + 3));
            
            // Δημιουργία 4-gram (π.χ. ATGC)
            if (i + 4 <= len) results.add(sequence.substring(i, i + 4));
        }
        return results;
    }

    public static class DNAMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text ngramKey = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Καθαρισμός γραμμής από κενά διαστήματα
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // Εξαγωγή όλων των πιθανών n-grams από την τρέχουσα γραμμή
            ArrayList<String> ngrams = findNgrams(line);
            
            // Εγαγωγή κάθε n-gram με τιμή 1 για την επακόλουθη άθροιση στον Reducer
            for (String n : ngrams) {
                ngramKey.set(n);
                context.write(ngramKey, one);
            }
        }
    }

    /**
     * Reducer:  αθροιστής εμφανίσεων.
     * Συγκεντρώνει όλους τους "άσους" που αντιστοιχούν στο ίδιο n-gram.
     */
    public static class DNAReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable finalSum = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int total = 0;
            // Διατρέχουμε το Iterable για το συγκεκριμένο κλειδί
            for (IntWritable val : values) {
                total += val.get();
            }
            finalSum.set(total);
            // Τελική έξοδος στο HDFS: (n-gram, συνολικό_πλήθος)
            context.write(key, finalSum);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job dnaJob = Job.getInstance(conf, "DNA N-gram Counter");
        
        dnaJob.setJarByClass(DNASequenceAnalyzer.class);
        dnaJob.setMapperClass(DNAMapper.class);
        dnaJob.setReducerClass(DNAReducer.class);

        // Καθορισμός των τύπων δεδομένων των Key-Value pairs της εξόδου
        dnaJob.setOutputKeyClass(Text.class);
        dnaJob.setOutputValueClass(IntWritable.class);

        // Ορισμός διαδρομών εισόδου (ecoli.txt) και εξόδου
        FileInputFormat.addInputPath(dnaJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(dnaJob, new Path(args[1]));

        // Εκτέλεση και αναμονή ολοκλήρωσης
        System.exit(dnaJob.waitForCompletion(true) ? 0 : 1);
    }
}