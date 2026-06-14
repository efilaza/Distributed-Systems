package question3;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Random;

public class Producer {

    // Πόσες φορές θα στείλει τιμές ο producer στον server
    private static final int REPEAT = 10;

    private final String name; // Όνομα του producer (για σκοπούς εμφάνισης και καταγραφής)

    // Πίνακας με τα hostnames ή τις IP διευθύνσεις των servers
    public final String[] hosts;

    // Πίνακας με τις αντίστοιχες πόρτες των servers
    public final int[] ports;

    public Producer(String name, String[] hosts, int[] ports) {
        this.name = name;

        // Έλεγχος ότι ο αριθμός των hosts και των ports είναι ίσος
        if (hosts.length != ports.length) {
            throw new RuntimeException(name + " Server hosts and ports lists have different sizes.");
        }

        this.hosts = hosts;
        this.ports = ports;

        startCommunication(); // Ξεκινάει η επικοινωνία με τους servers
    }

    private void startCommunication() {
        for (int i = 0; i < REPEAT; i++) {
            try {
                // Ο producer περιμένει για τυχαίο χρονικό διάστημα (1–10 δευτερόλεπτα)
                Thread.sleep(1000 * (new Random().nextInt(10) + 1));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Socket socket = null;

            // Επιλογή τυχαίου server από τη λίστα
            int serverIndex = new Random().nextInt(hosts.length);
            System.out.println(name + " Attempting to connect to host " + hosts[serverIndex] + " on port " + ports[serverIndex]);
            try {
                // Δημιουργία σύνδεσης με τον επιλεγμένο server
                socket = new Socket(hosts[serverIndex], ports[serverIndex]);
                System.out.println(name + " connected " + i);
            } catch (IOException e) {
                // Σε περίπτωση αποτυχίας σύνδεσης, εμφανίζεται μήνυμα και το πρόγραμμα τερματίζεται
                System.err.println(name + " connection error " + i);
                System.exit(1);
            }

            PrintWriter out = null;
            try {
                // Δημιουργία ροής εξόδου για αποστολή δεδομένων στον server
                out = new PrintWriter(socket.getOutputStream(), true);
            } catch (IOException e) {
                System.err.println(name + " Couldn't get I/O");
                System.exit(1);
            }

            // Δημιουργία τυχαίας τιμής μεταξύ 10 και 100 (θετική – χαρακτηριστικό του producer)
            int value = new Random().nextInt(91) + 10;

            // Αποστολή της τιμής στον server
            out.println(value);

            // Κλείσιμο ροής εξόδου και socket
            try {
                out.close();
                socket.close();
            } catch (IOException e) {
                System.err.println(name + " Error when closing sockets");
            }
        }

        // Μήνυμα ολοκλήρωσης αποστολής τιμών
        System.out.println(name + " finished");
    }
}
