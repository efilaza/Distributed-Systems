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
import java.util.StringTokenizer;

/**
 * ΥΠΟΕΡΓΑΣΙΑ 1
 * Υλοποίηση παραγωγής Numeronyms με παραμετρικό φίλτρο k.
 */
public class NumeronymCounter {

    // Βοηθητική μέθοδος για τον σχηματισμό του numeronym
    private static String createNumeronym(String word) {
        if (word == null || word.length() < 3) {
            return null; // Αγνοούμε λέξεις με λιγότερους από 3 χαρακτήρες
        }
        // Σχηματισμός: 1ος χαρακτήρας + (μήκος-2) + τελευταίος χαρακτήρας
        return word.charAt(0) + String.valueOf(word.length() - 2) + word.charAt(word.length() - 1);
    }

    public static class NumeronymMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1); // Σταθερή τιμή 1 για κάθε εμφάνιση
        private Text numeronymKey = new Text(); // Βελτιστοποιημένος τύπος Text του Hadoop

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 1. Καθαρισμός: Αφαίρεση στίξης και μετατροπή σε κεφαλαία
            String line = value.toString().replaceAll("\\p{Punct}", " ").toUpperCase();

            // 2. Τεμαχισμός γραμμής σε λέξεις
            StringTokenizer tokenizer = new StringTokenizer(line);

            while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();	
				String num = createNumeronym(word);				
				if (num != null) {
					numeronymKey.set(num);
					context.write(numeronymKey, one);
				}
			}
        }
    }

    public static class NumeronymReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // Ανάκτηση της παραμέτρου k από το Configuration. Αν δεν δοθεί, default τιμή το 10
            int k = context.getConfiguration().getInt("k_parameter", 10);

            int sum = 0;
            // Άθροιση όλων των εμφανίσεων για το συγκεκριμένο numeronym
            for (IntWritable val : values) {
                sum += val.get();
            }

            // Εφαρμογή του παραμετρικού φίλτρου k
            if (sum >= k) {
                result.set(sum);
                context.write(key, result); // Εγγραφή στο HDFS μόνο αν ικανοποιείται το k
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Έλεγχος ορισμάτων: input_path, output_path, k_value
        if (args.length < 2) {
            System.err.println("Usage: NumeronymCounter <input path> <output path> [<k_value>]");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // Αν ο χρήστης έδωσε 3ο όρισμα, το ορίζουμε ως k
        if (args.length >= 3) {
            conf.setInt("k_parameter", Integer.parseInt(args[2]));
        }

        Job job = Job.getInstance(conf, "Numeronym Analysis"); // Δημιουργία εργασίας
        job.setJarByClass(NumeronymCounter.class); // Ορισμός κλάσης Jar

        job.setMapperClass(NumeronymMapper.class); // Ορισμός Mapper
        job.setReducerClass(NumeronymReducer.class); // Ορισμός Reducer 

        job.setOutputKeyClass(Text.class); // Τύπος κλειδιού εξόδου 
        job.setOutputValueClass(IntWritable.class); // Τύπος τιμής εξόδου

        FileInputFormat.addInputPath(job, new Path(args[0])); // Διαδρομή εισόδου στο HDFS 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Διαδρομή εξόδου στο HDFS

        // Αναμονή ολοκλήρωσης και έξοδος 
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}