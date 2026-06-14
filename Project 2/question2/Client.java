package question2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class Client {

    private Socket socket; // Socket για σύνδεση με τον server

    public Client(String hostname, int port) {
        System.out.println("Attempting to connect to host " + hostname + " on port " + port);

        try {
            socket = new Socket(hostname, port); // Προσπάθεια σύνδεσης με τον server
            System.out.println("Client: Connected");
        } catch (IOException e) {
            System.err.println("Client: Connection error");
            System.exit(1); // Τερματισμός αν αποτύχει η σύνδεση
        }
    }

    public void startCommunication() {
        PrintWriter out = null;
        BufferedReader in = null;
        BufferedReader stdIn = null;

        try {
            out = new PrintWriter(socket.getOutputStream(), true); // Ροή εξόδου προς τον server
            in = new BufferedReader(new InputStreamReader(socket.getInputStream())); // Ροή εισόδου από τον server
            stdIn = new BufferedReader(new InputStreamReader(System.in)); // Ροή εισόδου από το πληκτρολόγιο

            System.out.println("Type command (e.g., 1,5,10 | 3,5 | 0,0 to quit):");

            String userInput;
            while ((userInput = stdIn.readLine()) != null) { // Ανάγνωση εντολών από χρήστη
                out.println(userInput); // Αποστολή εντολής στον server

                if (userInput.equals("0,0")) { // Αν σταλεί "0,0", γίνεται έξοδος
                    System.out.println("Client: Exit command received. Terminating...");
                    break;
                }

                String response = in.readLine(); // Λήψη απάντησης από τον server
                System.out.println("Server response: " + response);
                System.out.print("Next command: ");
            }

        } catch (IOException e) {
            System.err.println("Client: Error during communication"); // Σφάλμα κατά την επικοινωνία
        } finally {
            try {
                // Κλείσιμο όλων των ροών και του socket
                if (out != null) out.close();
                if (in != null) in.close();
                if (stdIn != null) stdIn.close();
                socket.close();
            } catch (IOException e) {
                System.err.println("Client: Error closing resources");
            }
        }
    }

    public static void main(String[] args) {
        String host = "127.0.0.1";  // Προεπιλεγμένο host (localhost)
        int port = 9999;            // Προεπιλεγμένη θύρα

        // Αν δοθεί ένα όρισμα, χρησιμοποιείται ως IP ή hostname
        if (args.length >= 1) {
            host = args[0];
        }

        new Client(host, port).startCommunication(); // Εκκίνηση client και έναρξη επικοινωνίας
    }
}



