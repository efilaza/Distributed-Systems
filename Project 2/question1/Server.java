package question1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Hashtable;

public class Server {

    private static final int PORT = 9999;

    // Hashtable για αποθήκευση (key, value)
    private final Hashtable<Integer, Integer> hashTable;


    private ServerSocket serverSocket;
    private Socket clientSocket;

    //Constuctor
    public Server(int port) {
        hashTable = new Hashtable<>();

        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Server started on port " + port);
        } catch (IOException e) {
            System.err.println("Could not listen on port: " + port);
            System.exit(1);
        }
    }


    public void acceptClient() {
        System.out.println("Waiting for client...");

        try {
            clientSocket = serverSocket.accept();
            System.out.println("Client connected");

            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

            processInput(in, out);

            in.close();
            out.close();
            clientSocket.close();
            serverSocket.close();

        } catch (IOException e) {
            System.err.println("Server error during communication");
        }
    }

    private void processInput(BufferedReader in, PrintWriter out) throws IOException {
        String inputLine;

        while ((inputLine = in.readLine()) != null) {
            System.out.println("Server received: " + inputLine);

            //Διαχωρισμός του string με βάση το κομμα σε ένα πίνακα part[]
            String[] parts = inputLine.split(",");

            //ελεγχος των τιμών αν είναι μικρότερο του 2 ή μεγαλύτερο του 3 δεν είναι αποδεκτό
            if (parts.length < 2 || parts.length > 3) {
                out.println(0);
                continue;
            }

            int command;
            int key;
            int value = 0;

            try {

                //το part[0] αποτελεί την εντολή
                command = Integer.parseInt(parts[0].trim());

                //το part[1] αποτελεί το κλειδί του hashtable
                key = Integer.parseInt(parts[1].trim());

                //Αν υπάρχει και prt[2] τότε αυτό αποτελεί το value
                if (parts.length == 3) {
                    value = Integer.parseInt(parts[2].trim());
                }
            } catch (NumberFormatException e) {
                out.println(0);
                continue;
            }

            switch (command) {
                case 0: // exit
                    if (key == 0) {
                        System.out.println("Server shutting down");
                        return;
                    }
                    out.println(0);
                    break;

                case 1: // insert
                    out.println(insert(key, value));
                    break;

                case 2: // delete
                    out.println(delete(key));
                    break;

                case 3: // search
                    out.println(search(key));
                    break;

                default:
                    out.println(0);
            }
        }
    }

    //Μέθοδος για την είσοδο ζεύγους κλειδί-τιμή στο HashTable
    private int insert(int key, int value) {
        //Έλεγχος τιμών
        if (key < 0 || value <= 0) {
            return 0;
        }
        //Είσοδος ζεύγους
        hashTable.put(key, value);
        System.out.println("Inserted (" + key + "," + value + ")");
        return 1;
    }

    //Μέθοδος για την διαγραφή ζεύγους με βάση το κλειδί
    private int delete(int key) {
        //Ελεγχος για αρνητική τιμή κλειδιόυ
        if (key < 0) {
            return 0;
        }

        //Διαγραφή κλειδιού
        if (hashTable.remove(key) != null) {
            System.out.println("Deleted key " + key);
            return 1;
        }
        return 0;
    }

    //Μέθοδος για την αναζήτηση ενός κλειδιού
    private int search(int key) {
        //Ελεγχός της τιμής του κλειδιού αν είναι αρνητική
        if (key < 0) {
            return 0;
        }


        Integer value = hashTable.get(key);
        System.out.println("Search key " + key + " -> " + value);
        //Αν βρεθεί το κλειδί επιστρέφει την αντίστοιχη τιμή αλλιώς επιστέφει 0.
        return value != null ? value : 0;
    }


    //Μέθοδος main για την εκκίνηση του server
    public static void main(String[] args) {
        new Server(PORT).acceptClient();
    }

}

