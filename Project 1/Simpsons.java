import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class Simpsons {

    private static final int NUM_THREADS = 8;
    private static final String CSV_PATH = "src/simpsons_script_lines.csv";

    //Φιλτραρουμε μόνο τους 4 χαρακτήρες
    private static final List<String> TARGET_CHARACTERS = new ArrayList<>();
    static {
        TARGET_CHARACTERS.add("bart");
        TARGET_CHARACTERS.add("homer");
        TARGET_CHARACTERS.add("marge");
        TARGET_CHARACTERS.add("lisa");
    }

    public static void main(String[] args) {
        List<String> lines = loadCsvFile(CSV_PATH);
        if (lines.isEmpty()) {
            System.err.println("Το αρχείο δεν διαβάστηκε σωστά.");
            System.exit(0);
        }

        // Αφαίρεση επικεφαλίδας
        lines.remove(0);

        // Δημιουργία  αντικειμένου για αποθήκευση στατιστικών
        Stats statistics = new Stats();


        // Δημιουργία thread pool
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

        //Χρόνος έναρξης
        long startTime = System.currentTimeMillis();

        int chunkSize = lines.size() / NUM_THREADS;


        //Χωρίζει τις γραμμές σε ισομεγέθη chunks
        //Το τελευταίο νήμα παίρνει τις υπόλοιπες γραμμές, αν δεν διαιρείται ακριβώς
        for (int i = 0; i < NUM_THREADS; i++) {
            int start = i * chunkSize;
            int finish;
            if (i == NUM_THREADS - 1) {
                finish = lines.size();
            } else {
                finish = start + chunkSize;
            }

            List<String> subList = lines.subList(start,finish);
            Runnable task = new ProcessLines(subList, statistics);
            executor.execute(task);
        }

        //Ο Executor δεν δέχεται άλλα threads.
        //Περιμένει 10 λεπτά για την ολοκλήρωση όλων των threads
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Χρόνος Λήξης
        long endTime = System.currentTimeMillis();

        System.out.println("\nΔιάρκεια:  " + (endTime - startTime) + "msec" + " με αριθμό νημάτων: " + NUM_THREADS);

        // Εκτύπωση στατιστικών
        statistics.printResults();
    }


    //Βοηθητική μέθοδος για την ανάγνωση του αρχείου csv με χρήση BufferReader
    private static List<String> loadCsvFile(String path) {
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        } catch (IOException e) {
            System.err.println("Σφάλμα ανάγνωσης αρχείου: " + e.getMessage());
        }
        return lines;
    }

    // Αυτή η κλάση υλοποιεί το Runnable και αντιπροσωπεύει τη δουλειά που θα κάνει κάθε νήμα
    // Κάθε αντικείμενο αυτής της κλάσης επεξεργάζεται ένα τμήμα γραμμών του αρχείου.
    static class ProcessLines implements Runnable {
        private final List<String> lines;
        private final Stats statistics;

        //Constuctor
        public ProcessLines(List<String> lines, Stats statistics) {
            this.lines = lines;
            this.statistics = statistics;
        }

        @Override
        public void run() {
            for (String line : lines) {
                String[] columns = line.split(",", -1); //χωρίζει τις γραμμές με βάση το κόμμα
                if (columns.length != 9){ // αν δεν έχει 9 πεδία την αγνοεί
                    continue;
                }


                String episode = columns[1].trim();
                String location = columns[4].trim();
                String speaker = columns[5].toLowerCase().trim();
                String dialogue = columns[7].trim();
                String wordCountStr = columns[8].trim();

                try {
                    int wordCount = Integer.parseInt(wordCountStr);
                    if (wordCount < 10000) {
                        statistics.addWordsToEpisode(episode, wordCount);
                    }
                }catch (NumberFormatException e){
                    }



                statistics.incrementLocationCount(location);

                for (String character : TARGET_CHARACTERS) {
                    if (speaker.contains(character)) {
                        statistics.countWordsForCharacter(character, dialogue);
                    }
                }
            }
        }
    }

    // Κοινόχρηστο αντικείμενο για αποθήκευση στατιστικών
    static class Stats {
        private final Map<String, Integer> episodeWordCounts = new ConcurrentHashMap<>();
        private final Map<String, Integer> locationCounts = new ConcurrentHashMap<>();
        private final Map<String, Map<String, Integer>> characterWordCounts = new ConcurrentHashMap<>();

        //Constructor
        //Για κάθε ένα από τους 4 χαρακτήρες δημιουργείται ένα
        //concurrent hashmap και τον αποθηκεύει μέσα στο map characterWordCounts
        public Stats() {
            for (String ch : TARGET_CHARACTERS) {
                characterWordCounts.put(ch, new ConcurrentHashMap<>());
            }
        }

        public void addWordsToEpisode(String episode, int count) {
            if (episodeWordCounts.containsKey(episode)) {
                int currentCount = episodeWordCounts.get(episode);
                episodeWordCounts.put(episode, currentCount + count);
            } else {
                episodeWordCounts.put(episode, count);
            }
        }


        public void incrementLocationCount(String location) {
            if (locationCounts.containsKey(location)) {
                int current = locationCounts.get(location);
                locationCounts.put(location, current + 1);
            } else {
                locationCounts.put(location, 1);
            }
        }


        public void countWordsForCharacter(String character, String dialogue) {
            Map<String, Integer> wordMap = characterWordCounts.get(character);
            String[] words = dialogue.toLowerCase().split("\\s+");

            for (String word : words) {
                if (word.length() >= 5) {
                    if (wordMap.containsKey(word)) {
                        int currentCount = wordMap.get(word);
                        wordMap.put(word, currentCount + 1);
                    } else {
                        wordMap.put(word, 1);
                    }
                }
            }
        }


        public void printResults() {
            System.out.println("Ερώτημα 1: Επεισόδιο με τις περισσότερες λέξεις");
            String maxEpisode = null;
            int maxWords = 0;
            for (Map.Entry<String, Integer> entry : episodeWordCounts.entrySet()) {
                if (entry.getValue() > maxWords) {
                    maxEpisode = entry.getKey();
                    maxWords = entry.getValue();
                }
            }
            if (maxEpisode != null) {
                System.out.println(maxEpisode + ": " + maxWords + " λέξεις");
            }


            System.out.println("\nΕρώτημα 2: Τοποθεσία με τις περισσότερες ατάκες");
            String maxLocation = null;
            int maxCount = 0;
            for (Map.Entry<String, Integer> entry : locationCounts.entrySet()) {
                if (entry.getValue() > maxCount) {
                    maxLocation = entry.getKey();
                    maxCount = entry.getValue();
                }
            }
            if (maxLocation != null) {
                System.out.println(maxLocation + ": " + maxCount + " ατάκες");
            }


            System.out.println("\nΕρώτημα 3: Πιο συχνή λέξη ανά χαρακτήρα (>=5 χαρακτήρες):");
            for (String character : TARGET_CHARACTERS) {
                Map<String, Integer> wordMap = characterWordCounts.get(character);
                String mostFrequentWord = null;
                int maxFrequency = 0;
                for (Map.Entry<String, Integer> entry : wordMap.entrySet()) {
                    if (entry.getValue() > maxFrequency) {
                        mostFrequentWord = entry.getKey();
                        maxFrequency = entry.getValue();
                    }
                }
                if (mostFrequentWord != null) {
                    System.out.println(character + ": '" + mostFrequentWord + "' (" + maxFrequency + " φορές)");
                }
            }

        }
    }
}