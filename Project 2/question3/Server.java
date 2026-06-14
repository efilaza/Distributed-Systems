package question3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Server {

    private final String name; // Όνομα για αναγνώριση του server (χρήσιμο στα logs)
    private ServerSocket serverSocket = null;

    // Αποθηκευτικός χώρος (storage) που τροποποιείται από producers και consumers
    private int storage;

    // Αντικείμενο για συγχρονισμό κατά την πρόσβαση στο storage
    private final Object lock = new Object();

    // Executor για τη διαχείριση πολλών clients παράλληλα με νήματα (threads)
    private final ExecutorService executor = Executors.newCachedThreadPool();

    public Server(String name, int port) {
        this.name = name;

        // Τυχαία αρχικοποίηση της μεταβλητής storage (1 έως 1000)
        storage = new Random().nextInt(1000) + 1;

        try {
            serverSocket = new ServerSocket(port); // Άνοιγμα socket στην καθορισμένη πόρτα
        } catch (IOException e) {
            System.err.println(name + " could not listen on port: " + port);
            System.exit(1);
        }

        System.out.println(name + " started");
        acceptClient(); // Κλήση της μεθόδου που δέχεται συνεχώς clients
    }

    private void acceptClient() {
        while (true) {
            try {
                // Αποδοχή νέου client
                Socket clientSocket = serverSocket.accept();

                // Δημιουργία νέου νήματος για κάθε σύνδεση μέσω executor
                executor.submit(new ServerTask(clientSocket));
            } catch (IOException e) {
                System.err.println(name + " accept failed.");
                System.exit(1);
            }
        }
    }

    // Εσωτερική κλάση που εκτελείται για κάθε σύνδεση client
    private class ServerTask implements Runnable {
        private final Socket clientSocket;

        public ServerTask(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {
            BufferedReader in = null;
            String inputLine = null;

            try {
                // Ανάγνωση εισερχόμενης τιμής από τον client
                in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                inputLine = in.readLine();

                int number = Integer.parseInt(inputLine); // Μετατροπή σε ακέραιο

                // Εκτύπωση ανάλογα με το αν πρόκειται για producer ή consumer
                if (number > 0) {
                    System.out.println(name + " P Value: " + inputLine);
                } else {
                    System.out.println(name + " C Value: " + inputLine);
                }

                // Συγχρονισμένη πρόσβαση στον αποθηκευτικό χώρο
                synchronized (lock) {
                    if (storage + number > 1000) {
                        System.out.println(name + " MAX VALUE LIMIT: " + storage);
                    } else if (storage + number < 1) {
                        System.out.println(name + " MIN VALUE LIMIT: " + storage);
                    } else {
                        storage += number;
                        System.out.println(name + " NEW VALUE: " + storage);
                    }
                }

            } catch (IOException e) {
                System.err.println(name + " IOException...");
            } catch (ArithmeticException | NumberFormatException e) {
                System.err.println(name + " Invalid data: " + inputLine);
            } finally {
                // Κλείσιμο του socket και της ροής εισόδου
                try {
                    if (in != null) in.close();
                    clientSocket.close();
                } catch (IOException e) {
                    System.err.println(name + " Error when closing sockets");
                }
            }
        }
    }
}
