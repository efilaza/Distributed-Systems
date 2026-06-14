import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

/**
 * ΥΠΟΕΡΓΑΣΙΑ 4 - ΕΡΩΤΗΜΑ 2
 * Ανάλυση Γράφου με χρήση Chained MapReduce για δυναμικό φιλτράρισμα.
 * . Να αλλάξετε τον κώδικα στο 1 έτσι ώστε στο τέλος να δίνονται στην έξοδο μόνο οι 
    κορυφές που έχουν μέσο βαθμό μεγαλύτερο από το μέσο όρων των βαθμών όλων των 
    κορυφών. 
 */
public class AdvancedGraphAnalyzer {

    private static final String INTERMEDIATE_DIR = "temp_metrics";

    // JOB 1: Mapper - Συλλογή στοιχείων για τον υπολογισμό του μέσου όρου
    public static class AvgPreparationMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(" ");
            if (parts.length == 3) {
                // Εγαγωγή κορυφών για καταμέτρηση μοναδικών κόμβων (πχ ζεύγος: (key = "NODE_COUNT",value= "102"))
				//Με αυτών των τρόπο θα περάσουν όλοι οι κόμβοι σε ένα reducer για την καταμέτρησή τους
                context.write(new Text("NODE_COUNT"), new Text(parts[0]));
                context.write(new Text("NODE_COUNT"), new Text(parts[1]));

                // Εξαγωγή πιθανότητας δύο φορές (μία για κάθε άκρο) για το συνολικό άθροισμα 
                context.write(new Text("TOTAL_PROB"), new Text(parts[2]));
                context.write(new Text("TOTAL_PROB"), new Text(parts[2]));
            }
        }
    }

    // JOB 1: Reducer - Υπολογισμός |V| και ΣP
    public static class AvgPreparationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (key.toString().equals("NODE_COUNT")) {
                // Χρήση HashSet για την εύρεση των μοναδικών κορυφών 
                HashSet<String> uniqueNodes = new HashSet<>();
                for (Text val : values) uniqueNodes.add(val.toString());
				
				//uniqueNodes.size() δίνει τον συνολικό αριθμό των στοιχείων του hashSet 
				//συνεπώς τον συνολικό αριθμό των μοναδικών κόμβων.
                context.write(key, new DoubleWritable(uniqueNodes.size())); 
            } else {
                // Άθροιση όλων των πιθανοτήτων
                double totalSum = 0;
                for (Text val : values) totalSum += Double.parseDouble(val.toString());
                context.write(key, new DoubleWritable(totalSum));
            }
        }
    }
	
	/* 
	Η υλοποίηση των Μapper και Reducer για το JOB 2 είναι ίδια με του υποερωτήματος 1 
	*/
		
    // JOB 2: Mapper - Υπολογισμός βαθμού ανά κορυφή
    public static class NodeDegreeMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(" ");
            if (parts.length == 3) {
                double p = Double.parseDouble(parts[2]);
                context.write(new Text(parts[0]), new DoubleWritable(p));
                context.write(new Text(parts[1]), new DoubleWritable(p));
            }
        }
    }

    // JOB 2: Reducer - Φιλτράρισμα βάσει του καθολικού μέσου όρου
    public static class FinalFilterReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            // Ανάκτηση του μέσου όρου από το Configuration 
            double globalAvg = context.getConfiguration().getDouble("GLOBAL_AVG", 0.0);
            double currentSum = 0;
            for (DoubleWritable val : values)
				currentSum += val.get();

            // Φιλτράρισμα: Εξαγωγή μόνο αν ο βαθμός είναι μεγαλύτερος από τον μέσο όρο 
            if (currentSum > globalAvg) {
                context.write(key, new DoubleWritable(currentSum));
            }
        }
    }

    /**
     * Βοηθητική μέθοδος για την ανάγνωση των μετρικών από το HDFS και υπολογισμό του μέσου όρου .
     */
    private static double getCalculatedMean(Configuration conf) throws IOException {
        double nodes = 0, probSum = 0;
		
        Path hdfsPath = new Path(INTERMEDIATE_DIR + "/part-r-00000");
		
        FileSystem fs = FileSystem.get(conf); //Διαβάζει από το conf ότι το filesytem είναι το hdfs

        if (!fs.exists(hdfsPath)) throw new IOException("Job 1 output not found!");

        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(hdfsPath)));
        String line;
        while ((line = br.readLine()) != null) {
            String[] p = line.split("\t");
			
            if (p[0].equals("NODE_COUNT")) nodes = Double.parseDouble(p[1]); // Αριθμός κόμβων όπως διαβάζεται από το αρχείο
            else if (p[0].equals("TOTAL_PROB")) probSum = Double.parseDouble(p[1]); // Αθροισμα πιθανοτήτων όπως διαβάζεται από το αρχείο.
        }
        br.close();
        return probSum / nodes; // Υπολογισμός Μέσου όρου
	}
	
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // ΕΚΤΕΛΕΣΗ JOB 1
        Job job1 = Job.getInstance(conf, "Phase 1: Metrics");
        job1.setJarByClass(AdvancedGraphAnalyzer.class);
        job1.setMapperClass(AvgPreparationMapper.class);
        job1.setReducerClass(AvgPreparationReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(INTERMEDIATE_DIR));

        if (!job1.waitForCompletion(true)) System.exit(1);

        // ΜΕΤΑΒΑΣΗ: Υπολογισμός μέσου όρου και πέρασμα στο επόμενο Job
        double dynamicAvg = getCalculatedMean(conf);
		
		// Ορίζει την global μεταβλητή στον Configuration με όνομα "GLOBAL_AVG" ίση 
		//με τον μέσο όρο ώστε να είναι προσβάσιμη από το δεύτερο JOB.
		
        conf.setDouble("GLOBAL_AVG", dynamicAvg); 

        // ΕΚΤΕΛΕΣΗ JOB 2
        Job job2 = Job.getInstance(conf, "Phase 2: Filtering");
        job2.setJarByClass(AdvancedGraphAnalyzer.class);
        job2.setMapperClass(NodeDegreeMapper.class);
        job2.setReducerClass(FinalFilterReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}