import java.util.Random;
import java.util.concurrent.*;

public class ClothingStoreSimulation {

    // Σταθερές
    private static final int MAX_CUSTOMERS = 100;
    private static final int MAX_STORE_CAPACITY = 40;
    private static final int MAX_FITTING_ROOMS = 5;
    private static final int MAX_CASHIER_QUEUE = 10;
    private static final int CUSTOMER_INTERVAL_MIN = 2000;
    private static final int CUSTOMER_INTERVAL_MAX = 5000;
    private static final int FITTING_TIME_MIN = 3000;
    private static final int FITTING_TIME_MAX = 10000;
    private static final int CASHIER_TIME = 5000;

    // Thread pool που δημιουργεί νήματα ανάλογα με τις ανάγκες
    private static final ExecutorService executor = Executors.newCachedThreadPool();

    // Σημαφόροι για τον έλεγχο της χωρητικότητας σε κάθε στάδιο
    private static final Semaphore storeEntrance = new Semaphore(MAX_STORE_CAPACITY);
    private static final Semaphore fittingRoomsMen = new Semaphore(MAX_FITTING_ROOMS);
    private static final Semaphore fittingRoomsWomen = new Semaphore(MAX_FITTING_ROOMS);
    private static final Semaphore cashierSemaphore = new Semaphore(MAX_CASHIER_QUEUE);

    // Κλείδωμα για το ταμείο, ώστε μόνο ένας πελάτης να εξυπηρετείται κάθε φορά
    private static final Object lock = new Object();

    public static void main(String[] args) throws InterruptedException {
        Random rand = new Random();

        for (int i = 1; i <= MAX_CUSTOMERS; i++) {
            boolean isWoman = (i % 2 != 0); // Περιττός αριθμός = Γυναίκα, Άρτιος = Άνδρας
            Customer c = new Customer(i, isWoman);

            // Δημιουργία και εκτέλεση νέου task για κάθε πελάτη
            Runnable task  = new CustomerTask(c);
            executor.submit(task);

            // Τυχαία καθυστέρηση μεταξύ αφίξεων πελατών
            int wait = CUSTOMER_INTERVAL_MIN + rand.nextInt(CUSTOMER_INTERVAL_MAX - CUSTOMER_INTERVAL_MIN);
            Thread.sleep(wait);
        }

        // Τερματισμός του executor αφού ολοκληρωθούν όλα τα tasks
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // Κλάση που εκπροσωπεί έναν πελάτη
    private static class Customer {
        private final int id;
        private final boolean isWoman;

        //Constructor
        public Customer(int id, boolean isWoman) {
            this.id = id;
            this.isWoman = isWoman;
        }

        @Override
        public String toString() {
            if (isWoman)
                return "Γυναίκα " + id;
            else
                return "Άνδρας " + id;
        }
    }

    // Κλάση που υλοποιεί Runnable για να εξυπηρετεί κάθε πελάτη ως ξεχωριστό task
    private static class CustomerTask implements Runnable {
        private final Customer customer;

        //Constructor
        public CustomerTask(Customer customer) {
            this.customer = customer;
        }

        @Override
        public void run() {
            processCustomer(customer);
        }
    }

    // Μέθοδος που διαχειρίζεται ολόκληρη τη ροή εξυπηρέτησης ενός πελάτη
    private static void processCustomer(Customer customer) {
        try {
            // Αναμονή για είσοδο στο κατάστημα
            if (storeEntrance.availablePermits() == 0) {
                System.out.println(customer + " περιμένει να μπει στο κατάστημα...");
            }
            storeEntrance.acquire();
            System.out.println(customer + " μπήκε στο κατάστημα. Διαθέσιμες θέσεις: " + storeEntrance.availablePermits());

            // Επιλογή δοκιμαστηρίου ανάλογα με το φύλο
            Semaphore fittingRoom;
            String gender;

            if (customer.isWoman) {
                fittingRoom = fittingRoomsWomen;
                gender = "Γ";
            } else {
                fittingRoom = fittingRoomsMen;
                gender = "Ά";
            }

            // Αναμονή για είσοδο στο δοκιμαστήριο
            if (fittingRoom.availablePermits() == 0) {
                System.out.println(customer + " περιμένει για να μπει στο δοκιμαστήριο (" + gender + ")...");
            }

            fittingRoom.acquire();
            System.out.println(customer + " μπήκε στο δοκιμαστήριο (" + gender + "). Διαθέσιμες θέσεις: " + fittingRoom.availablePermits());

            // Προσομοίωση χρόνου στο δοκιμαστήριο
            Thread.sleep(randomBetween(FITTING_TIME_MIN, FITTING_TIME_MAX));
            System.out.println(customer + " τελείωσε από το δοκιμαστήριο.");

            // Αναμονή για ταμείο
            if (cashierSemaphore.availablePermits() == 0) {
                System.out.println(customer + " περιμένει για να πληρώσει στο ταμείο...");
            }

            cashierSemaphore.acquire();
            fittingRoom.release(); // Ελευθερώνει το δοκιμαστήριο
            System.out.println(customer + " περιμένει στο ταμείο. Διαθέσιμες θέσεις: " + cashierSemaphore.availablePermits());

            // Πληρωμή στο ταμείο (μόνο ένας κάθε φορά)
            synchronized (lock) {
                Thread.sleep(CASHIER_TIME);
                System.out.println(customer + " εξυπηρετήθηκε από το ταμείο.");
            }

            cashierSemaphore.release();

            // Αποχώρηση από το κατάστημα
            System.out.println(customer + " αποχωρεί από το κατάστημα.");
            storeEntrance.release();
            System.out.println("Διαθέσιμες θέσεις στο κατάστημα: " + storeEntrance.availablePermits());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Η εξυπηρέτηση του " + customer + " διακόπηκε.");
        }
    }

    // Βοηθητική μέθοδος για τυχαίο χρόνο εντός ορίων
    private static int randomBetween(int min, int max) {
        return new Random().nextInt(max - min + 1) + min;
    }
}
