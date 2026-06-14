package question1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class Client {


    Socket socket = null; // Το socket που θα συνδεθεί με τον server


    //Constructor
    public Client(String hostname, int port) {
        System.out.println("Attempting to connect to host " + hostname + " on port " + port);

        try {
            socket = new Socket(hostname, port); // Δημιουργεί σύνδεση TCP με τον server
            System.out.println("Client: Connected");
        } catch (IOException e) {
            System.err.println("Client: Connection error");
            System.exit(1);
        }
    }

    // Μέθοδος για αποστολή/λήψη εντολών και απαντήσεων
    public void startCommunication() {
        PrintWriter out = null;
        BufferedReader in = null;
        BufferedReader stdIn = null;

        try {
            //Αποστολή προς τον Server
            out = new PrintWriter(socket.getOutputStream(), true);
            //Ανάγνωση από τον server
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            //Ανάγνωση από το πληκτρολόγιο
            stdIn = new BufferedReader(new InputStreamReader(System.in));

            System.out.println("Type command (e.g., 1,5,10 | 3,5 | 0,0 to quit):");

            String userInput;

            while ((userInput = stdIn.readLine()) != null) {
                out.println(userInput);

                // Αν σταλεί 0,0 ο client τερματίζει
                if (userInput.equals("0,0")) {
                    System.out.println("Client: Exit command received. Terminating...");
                    break;
                }
                // Ανάγνωση απάντησης από τον server
                String response = in.readLine();
                System.out.println("Server response: " + response);
                System.out.print("Next command: ");
            }

        } catch (IOException e) {
            System.err.println("Client: Error during communication");
        } finally {
            try {
                //Κλείσιμο των ροών εισόδου/εξόδου και πληκτρολογίου
                if (out != null) out.close();
                if (in != null) in.close();
                if (stdIn != null) stdIn.close();
                socket.close();
            } catch (IOException e) {
                System.err.println("Client: Error closing resources");
            }
        }
    }


    //Μέθοδος Main για την εκκίνηση του Client
    public static void main(String[] args) {
        String host = "127.0.0.1";  // default
        int port = 9999;            // default
        //Αν δοθεί ένα όρισμα ορίζεται ως η IP του host
        if (args.length >= 1) {
            host = args[0];  // όρισμα: IP ή hostname
        }

        new Client(host, port).startCommunication();
    }
}


