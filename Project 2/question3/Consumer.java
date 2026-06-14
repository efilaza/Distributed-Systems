package question3;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Random;

public class Consumer {

    // Πόσες φορές θα στείλει τιμές ο consumer στον server
    private static final int REPEAT = 10;

    private final String name; // Όνομα του consumer (χρήσιμο για εμφάνιση logs)

    // Πίνακες με τα hostnames/IPs των διαθέσιμων servers
    public final String[] hosts;

    // Πίνακες με τις αντίστοιχες θύρες των servers
    public final int[] ports;

    public Consumer(String name, String[] hosts, int[] ports) {
        this.name = name;

        // Έλεγχος ώστε ο αριθμός των hosts να ταιριάζει με τον αριθμό των ports
        if (hosts.length != ports.length) {
            throw new RuntimeException(name + " Server hosts and ports lists have different sizes.");
        }

        this.hosts = hosts;
        this.ports = ports;

        startCommunication(); // Ξεκινά η διαδικασία επικοινωνίας με servers
    }

    private void startCommunication() {
        for (int i = 0; i < REPEAT; i++) {

            try {
                // Αναμονή για τυχαίο χρονικό διάστημα μεταξύ 1 και 10 δευτερολέπτων
                Thread.sleep(1000 * (new Random().nextInt(10) + 1));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Socket socket = null;

            // Επιλογή τυχαίου server από τις διαθέσιμες διευθύνσεις
            int serverIndex = new Random().nextInt(hosts.length);
            System.out.println(name + " Attempting to connect to host " + hosts[serverIndex] + " on port " + ports[serverIndex]);

            try {
                socket = new Socket(hosts[serverIndex], ports[serverIndex]); // Δημιουργία σύνδεσης με τον επιλεγμένο server
                System.out.println(name + " connected " + i);
            } catch (IOException e) {
                System.err.println(name + " connection error " + i); // Σφάλμα σύνδεσης
                System.exit(1);
            }

            PrintWriter out = null;
            try {
                out = new PrintWriter(socket.getOutputStream(), true); // Δημιουργία ροής εξόδου για αποστολή δεδομένων
            } catch (IOException e) {
                System.err.println(name + " Couldn't get I/O");
                System.exit(1);
            }

            // Παραγωγή μιας αρνητικής τιμής μεταξύ -10 και -100
            int value = new Random().nextInt(91) + 10; // τιμή μεταξύ 10 και 100
            value -= 2 * value; // μετατροπή σε αρνητική (consumer => αρνητικοί αριθμοί)

            out.println(value); // Αποστολή τιμής στον server

            try {
                out.close();       // Κλείσιμο της εξόδου
                socket.close();    // Κλείσιμο του socket
            } catch (IOException e) {
                System.err.println(name + " Error when closing sockets");
            }
        }

        // Τερματισμός λειτουργίας consumer
        System.out.println(name + " finished");
    }
}
