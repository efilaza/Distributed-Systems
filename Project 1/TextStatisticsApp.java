import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TextStatisticsApp {

    private static final int NUM_THREADS = 1;
    private static final int CALLS_PER_THREAD = 10;
    private static final String API_URL = "http://metaphorpsum.com/paragraphs/5";

    // Κοινές μεταβλητές για αποθήκευση αποτελεσμάτων
    private static int totalWords = 0;
    private static int totalWordLength = 0;
    private static long[] totalLetterCounts = new long[26];


    public static void main(String[] args) {
        //Εναρξη μέτρησης χρόνου
        long startTime = System.currentTimeMillis();

        //Δημιουργία thread pool με σταθερό αριθμό
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

        //Δημιουργία των νημάτων και αποστολή τους στον executor
        for (int i = 0; i < NUM_THREADS; i++) {
            Runnable task = new TextProcessingTask();
            executor.execute(task);
        }
        // O executor δεν δέχεται άλλα threads
        // Περιμένει για 10 λεπτά να ολοκληρωθούν όλα τα threads
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            System.err.println("Τα νήματα διακόπηκαν: " + e.getMessage());
        }

      printResults(startTime);
    }

    private static void printResults(long startTime) {
        System.out.println("\n--------- ΑΠΟΤΕΛΕΣΜΑΤΑ ---------");

        if (totalWords > 0) {
            double averageLength = (double) totalWordLength / totalWords;
            System.out.printf("Μέσος όρος μήκους λέξεων: %.2f\n", averageLength);
        } else {
            System.out.println("Δεν εντοπίστηκαν λέξεις.");
        }

        System.out.println("\nΠοσοστά εμφάνισης γραμμάτων:");
        long totalLetters = 0;
        for (long count : totalLetterCounts) {
            totalLetters += count;


        }

        for (int i = 0; i < 26; i++) {;
            double percentage = 100.0 * totalLetterCounts[i] / totalLetters;
            char letter = (char) ('a' + i);
            System.out.println(letter + ": " + String.format("%.2f", percentage) + "%");
        }

        long endTime = System.currentTimeMillis();
        System.out.println("\nΑριθμός νημάτων: " + NUM_THREADS);
        System.out.println("Χρόνος εκτέλεσης: " + (endTime - startTime) + " ms");
    }

    // ΕΣΩΤΕΡΙΚΗ ΚΛΑΣΗ
    // Αυτή η κλάση υλοποιεί το Runnable και αντιπροσωπεύει τη δουλειά που θα κάνει κάθε νήμα.
    static class TextProcessingTask implements Runnable {

        @Override
        public void run() {
            for (int i = 0; i < CALLS_PER_THREAD; i++) {
                String text = fetchTextFromAPI();
                if (text.isEmpty()) {
                    System.out.println("Το κείμενο είναι άδειο.");
                    continue;
                }

                // Αντικαθιστά τα σημεία στίξης με κενά
                String clean = text.replaceAll("\\p{Punct}", " ");

                // Διαχωρίζει το κείμενο σε λέξεις με βάση τα κενά
                String[] words = clean.split("\\s+");

                int localWordCount = 0;
                int localWordLength = 0;

                for (String word : words) {
                    if (!word.isEmpty()) {
                        localWordCount++;
                        localWordLength += word.length();
                    }
                }

                // Map για την καταμέτρηση γραμμάτων
                Map<Character, Integer> letterCounts = new HashMap<>();
                for (char c : clean.toLowerCase().toCharArray()) {
                    if (Character.isLetter(c)) {
                        letterCounts.put(c, letterCounts.getOrDefault(c, 0) + 1);
                    }
                }

                // Συγχρονισμός για την ενημέρωση των κοινών μεταβλητών
                synchronized (TextStatisticsApp.class) {
                    totalWords += localWordCount;
                    totalWordLength += localWordLength;
                    for (Map.Entry<Character, Integer> entry : letterCounts.entrySet()) {
                        char letter = entry.getKey();
                        int count = entry.getValue();
                        totalLetterCounts[letter - 'a'] += count;
                    }
                }
            }
        }

        // Ανάκτηση κειμένου από το API
        private String fetchTextFromAPI() {
            StringBuilder content = new StringBuilder();
            try {
                URL api = new URL(API_URL);
                BufferedReader reader = new BufferedReader(new InputStreamReader(api.openStream()));
                String line;
                while ((line = reader.readLine()) != null) {
                    content.append(line).append(" ");
                }
                reader.close();
            } catch (Exception e) {
                System.err.println("Σφάλμα ανάκτησης API: " + e.getMessage());
                return "";
            }
            return content.toString();
        }
    }

}
