
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MatrixVectorMultiplication {

    // Μέγιστη τιμή που μπορεί να πάρει κάθε στοιχείο του πίνακα ή του διανύσματος
    private static final int MAX_VALUE = 10;
    // Πλήθος νημάτων που θα χρησιμοποιηθούν για την εκτέλεση
    private static final int THREADS = 8;
    // Πλήθος γραμμών και στηλών του πίνακα
    private static final int TOTAL_ROWS = 1048576;
    private static final int TOTAL_COLUMNS = 1024;


    public static void main(String[] args) {

        // Δημιουργία τυχαίου πίνακα και διανύσματος εισόδου
        int[][] dataMatrix = createRandomMatrix(TOTAL_ROWS, TOTAL_COLUMNS);
        int[] inputVector = createRandomVector(TOTAL_COLUMNS);

        // Πίνακας εξόδου που θα αποθηκεύσει τα αποτελέσματα του πολλαπλασιασμού
        int[] outputVector = new int[TOTAL_ROWS];

        // Καθορισμός του πλήθους των γραμμών που θα επεξεργάζεται κάθε νήμα
        int chunk = TOTAL_ROWS / THREADS;

        // Δημιουργία executorService νημάτων σταθερού μεγέθους
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);

        // Καταγραφή του χρόνου έναρξης της εκτέλεσης
        long startTime = System.currentTimeMillis();

        // Δημιουργία και ανάθεση εργασιών στα νήματα
        for (int thread = 0; thread < THREADS; thread++) {

            // Υπολογισμός της πρώτης και τελευταίας γραμμής που θα επεξεργαστεί το κάθε νήμα
            int firstRow = thread * chunk;
            int lastRow;
            if (thread == THREADS - 1) {
                lastRow = TOTAL_ROWS;  // Αν είναι το τελευταίο νήμα, πάει μέχρι το τέλος του πίνακα
            } else {
                lastRow = firstRow + chunk;  // Αλλιώς, επεξεργάζεται μόνο το κομμάτι του
            }

            //Δημιουργία της εργασίας που θα εκτελεστεί από το thread
            Runnable task = new RowComputationTask(dataMatrix, inputVector, outputVector, firstRow, lastRow);
            // Εκχώρηση της εργασίας στο thread executorService
            executor.execute(task);
        }

        // Δεν υποβάλλονται άλλες εργασίες στο thread executorService
        executor.shutdown();

        // Αναμονή έως 10 λεπτά για να ολοκληρωθούν όλες οι εργασίες των νημάτων
        try {
            executor.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            System.err.println("Η εκτέλεση διεκόπη: " + e.getMessage());
        }

        // Καταγραφή χρόνου ολοκλήρωσης και υπολογισμός συνολικού χρόνου εκτέλεσης
        long endTime = System.currentTimeMillis();
        System.out.printf("Συνολικός χρόνος εκτέλεσης με %d νήματα: %d ms%n", THREADS, (endTime - startTime));
    }


    // ΕΣΩΤΕΡΙΚΗ ΚΛΑΣΗ
    // Αυτή η κλάση υλοποιεί το Runnable και αντιπροσωπεύει τη δουλειά που θα κάνει κάθε νήμα.
    // Κάθε αντικείμενο αυτής της κλάσης επεξεργάζεται ένα τμήμα γραμμών του πίνακα.
    static class RowComputationTask implements Runnable {

        private int[][] sourceMatrix;   // Ο πίνακας που θα πολλαπλασιαστεί
        private int[] multiplierVector; // Το διάνυσμα εισόδου
        private int[] resultVector;     // Το διάνυσμα αποτελέσματος
        private int start;              // Πρώτη γραμμή που θα επεξεργαστεί το νήμα
        private int end;                // Τελευταία γραμμή που θα επεξεργαστεί το νήμα

        // Constructor
        public RowComputationTask(int[][] sourceMatrix, int[] multiplierVector, int[] resultVector, int start, int end) {
            this.sourceMatrix = sourceMatrix;
            this.multiplierVector = multiplierVector;
            this.resultVector = resultVector;
            this.start = start;
            this.end = end;
        }

        // Η μέθοδος run() εκτελείται όταν ξεκινήσει το νήμα
        @Override
        public void run() {
            for (int i = start; i < end; i++) {
                // Για κάθε γραμμή του πίνακα, υπολογίζεται το εσωτερικό γινόμενο με το διάνυσμα
                resultVector[i] = computeDot(sourceMatrix[i], multiplierVector);
            }
        }


        // Μέθοδος που υπολογίζει το εσωτερικό γινόμενο (dot product)
        // Παίρνει δύο διανύσματα  και επιστρέφει το άθροισμα των γινομένων των στοιχείων τους.
        private int computeDot(int[] arrayA, int[] arrayB) {
            int total = 0;
            for (int i = 0; i < arrayA.length; i++) {
                total += arrayA[i] * arrayB[i];
            }
            return total;
        }
    }

    // Μέθοδος δημιουργίας πίνακα με τυχαίες τιμές
    private static int[][] createRandomMatrix(int rows, int columns) {
        Random generator = new Random();
        int[][] generatedMatrix = new int[rows][columns];
        for (int r = 0; r < rows; r++)
            for (int c = 0; c < columns; c++)
                generatedMatrix[r][c] = generator.nextInt(MAX_VALUE);
        return generatedMatrix;
    }

    // Μέθοδος δημιουργίας διανύσματος με τυχαίες τιμές
    private static int[] createRandomVector(int length) {
        Random generator = new Random();
        int[] generatedVector = new int[length];
        for (int i = 0; i < length; i++)
            generatedVector[i] = generator.nextInt(MAX_VALUE);
        return generatedVector;
    }
}
