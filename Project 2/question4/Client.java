package question4; // Ορισμός package του client

import javax.swing.*; // Swing για γραφικό περιβάλλον (dialog windows)
import java.net.MalformedURLException; // Exception για λάθος RMI URL
import java.rmi.NotBoundException; // Exception αν το όνομα δεν υπάρχει στο Registry
import java.rmi.RemoteException; // Βασική RMI exception (δικτύου)
import java.rmi.registry.LocateRegistry; // Για σύνδεση με RMI Registry
import java.rmi.registry.Registry; // Interface του RMI Registry

public class Client {

    private static final String[] COMMANDS = {"Insert", "Delete", "Search", "Exit"};
    // Διαθέσιμες εντολές του client

    public static void main(String[] args)
            throws MalformedURLException, RemoteException, NotBoundException {

        // Σύνδεση με το RMI Registry στη θύρα 1888
        Registry registry = LocateRegistry.getRegistry(1888);

        // Ανάκτηση stub (proxy) του remote object "Operations"
        Operations operations = (Operations) registry.lookup("Operations");

        // Δημιουργία frame για τα dialog windows
        JFrame frame = new JFrame();
        frame.setAlwaysOnTop(true);

        boolean finish = false;
        do {
            // Εμφάνιση menu επιλογών στον χρήστη
            Object selectionObject = JOptionPane.showInputDialog(
                    frame,
                    "Select command",
                    "Storage Operations",
                    JOptionPane.PLAIN_MESSAGE,
                    null,
                    COMMANDS,
                    COMMANDS[0]);

            // Αν πατηθεί Cancel, τερματισμός
            if (selectionObject == null) {
                break;
            }

            String command = selectionObject.toString();

            try {
                // Επιλογή ενέργειας βάσει εντολής
                switch (command) {
                    case "Insert":
                        handleInsert(frame, operations); // Remote insert
                        break;
                    case "Delete":
                        handleDelete(frame, operations); // Remote delete
                        break;
                    case "Search":
                        handleSearch(frame, operations); // Remote search
                        break;
                    case "Exit":
                        finish = true; // Τερματισμός client
                        break;
                }
            } catch (NumberFormatException e) {
                // Λάθος μετατροπής String σε int
                JOptionPane.showMessageDialog(frame,
                        "Invalid number format. Please enter integers only.");
            } catch (RemoteException e) {
                // Σφάλμα κατά την απομακρυσμένη κλήση
                JOptionPane.showMessageDialog(frame,
                        "Remote operation failed: " + e.getMessage());
            }
        } while (!finish);

        System.exit(0); // Τερματισμός εφαρμογής
    }

    private static void handleInsert(JFrame frame, Operations operations)
            throws RemoteException {

        // Εισαγωγή κλειδιού και τιμής από τον χρήστη
        String keyStr = JOptionPane.showInputDialog("Select key to insert");
        String valueStr = JOptionPane.showInputDialog("Select value to insert");

        // Ακύρωση αν πατηθεί Cancel
        if (keyStr == null || valueStr == null) return;

        // Μετατροπή εισόδου σε ακέραιους
        int key = Integer.parseInt(keyStr.trim());
        int value = Integer.parseInt(valueStr.trim());

        // Απομακρυσμένη κλήση insert στο server
        int result = operations.insert(key, value);

        // Έλεγχος αποτελέσματος
        if (result == 0) {
            JOptionPane.showMessageDialog(frame,
                    "Error while inserting key/value pair");
        } else {
            JOptionPane.showMessageDialog(frame, "Insert successful");
        }
    }

    private static void handleDelete(JFrame frame, Operations operations)
            throws RemoteException {

        // Εισαγωγή κλειδιού προς διαγραφή
        String keyStr = JOptionPane.showInputDialog("Select key to delete");
        if (keyStr == null) return;

        int key = Integer.parseInt(keyStr.trim());

        // Απομακρυσμένη κλήση delete
        int result = operations.delete(key);

        if (result == 0) {
            JOptionPane.showMessageDialog(frame, "Error while deleting key");
        } else {
            JOptionPane.showMessageDialog(frame, "Delete successful");
        }
    }

    private static void handleSearch(JFrame frame, Operations operations)
            throws RemoteException {

        // Εισαγωγή κλειδιού προς αναζήτηση
        String keyStr = JOptionPane.showInputDialog("Select key to search");
        if (keyStr == null) return;

        int key = Integer.parseInt(keyStr.trim());

        // Απομακρυσμένη κλήση search
        int value = operations.search(key);

        if (value == 0) {
            JOptionPane.showMessageDialog(frame,
                    "Key not found or error");
        } else {
            JOptionPane.showMessageDialog(frame,
                    "Value: " + value);
        }
    }
}

