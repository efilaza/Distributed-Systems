package question2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Hashtable;

class ClientHandler implements Runnable {

    private final Socket clientSocket;
    private final Hashtable<Integer, Integer> hashTable;

    public ClientHandler(Socket socket, Hashtable<Integer, Integer> table) {
        this.clientSocket = socket;
        this.hashTable = table;
    }

    @Override
    public void run() {
        acceptClient(); // Καλεί τη μέθοδο που χειρίζεται την επικοινωνία με τον client
    }

    public void acceptClient() {
        try (
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))
        ) {
            processInput(in, out); // Καλεί τη μέθοδο επεξεργασίας των εντολών
        } catch (IOException e) {
            System.err.println("Client communication error");
        } finally {
            try {
                clientSocket.close(); // Κλείσιμο του socket στο τέλος
            } catch (IOException e) {
                System.err.println("Failed to close client socket");
            }
        }
    }

    public void processInput(BufferedReader in, PrintWriter out) throws IOException {
        String inputLine;

        while ((inputLine = in.readLine()) != null) {
            System.out.println("Received: " + inputLine);
            String[] parts = inputLine.split(",");

            if (parts.length < 2 || parts.length > 3) {
                out.println(0);
                continue;
            }

            int command, key, value = 0;
            try {
                command = Integer.parseInt(parts[0].trim());
                key = Integer.parseInt(parts[1].trim());
                if (parts.length == 3) {
                    value = Integer.parseInt(parts[2].trim());
                }
            } catch (NumberFormatException e) {
                out.println(0);
                continue;
            }

            switch (command) {
                case 0:// exit
                    if (key == 0) {
                        System.out.println("Client requested termination");
                        return;
                    } else {
                        out.println(0);
                    }
                    break;
                case 1:// insert
                    out.println(insert(key, value));
                    break;
                case 2:// delete
                    out.println(delete(key));
                    break;
                case 3:// search
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
        if (key < 0 || value <= 0)
            return 0;

        //Είσοδος ζεύγους
        synchronized (hashTable) {
            hashTable.put(key, value);
        }
        System.out.println("Inserted (" + key + "," + value + ")");
        return 1;
    }
    //Μέθοδος για την διαγραφή ζεύγους με βάση το κλειδί
    private int delete(int key) {
        //Ελεγχος για αρνητική τιμή κλειδιόυ
        if (key < 0)
            return 0;

        //Διαγραφή κλειδιού
        synchronized (hashTable) {
            if (hashTable.remove(key) != null) {
                System.out.println("Deleted key " + key);
                return 1;
            }
        }
        return 0;
    }

    //Μέθοδος για την αναζήτηση ενός κλειδιού
    private int search(int key) {
        //Ελεγχός της τιμής του κλειδιού αν είναι αρνητική
        if (key < 0)
            return 0;
        synchronized (hashTable) {
            Integer val = hashTable.get(key);
            System.out.println("Search: " + key + " -> " + val);
            //Αν βρεθεί το κλειδί επιστρέφει την αντίστοιχη τιμή αλλιώς επιστέφει 0.
            return val != null ? val : 0;
        }
    }
}
