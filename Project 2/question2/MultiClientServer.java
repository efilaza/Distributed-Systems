package question2;

import java.io.*;
import java.net.*;
import java.util.Hashtable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultiClientServer {

    private static final int PORT = 9999; // Θύρα στην οποία "ακούει" ο server

    // Κοινός πίνακας κατακερματισμού για αποθήκευση των δεδομένων από όλους τους clients
    private final Hashtable<Integer, Integer> hashTable = new Hashtable<>();

    public void startServer() {
        // Δημιουργία thread pool για ταυτόχρονη εξυπηρέτηση πολλών clients
        ExecutorService pool = Executors.newCachedThreadPool();

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Server started on port " + PORT);

            // Άπειρος βρόχος: περιμένει και εξυπηρετεί συνεχώς νέες συνδέσεις
            while (true) {
                Socket clientSocket = serverSocket.accept(); // Αποδοχή σύνδεσης από νέο client
                System.out.println("New client connected");

                // Ανάθεση της διαχείρισης του client σε ξεχωριστό thread
                pool.execute(new ClientHandler(clientSocket, hashTable));
            }

        } catch (IOException e) {
            System.err.println("Server error: " + e.getMessage()); // Αν παρουσιαστεί σφάλμα κατά τη λειτουργία του server
        }
    }

    public static void main(String[] args) {
        new MultiClientServer().startServer(); // Εκκίνηση του server
    }
}


