package question4;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

// Διαχειρίζεται key/value αποθήκευση με thread-safe ConcurrentHashMap
public class Server extends UnicastRemoteObject implements Operations {

    // Thread-safe map για ταυτόχρονους clients
    private final ConcurrentMap<Integer, Integer> table = new ConcurrentHashMap<>();

    // Εκκίνηση RMI server και εγγραφή στο registry.
    public static void main(String[] args) {
        try {
            Registry registry = LocateRegistry.createRegistry(1888); // Start registry στον port 1888
            registry.rebind("Operations", new Server()); // Bind remote object με όνομα "Operations"
            System.out.println("Server ready...");
        } catch (Exception e) {
            System.err.println("Server exception: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Constructor: κάνει export το αντικείμενο ως Unicast remote object.
    protected Server() throws RemoteException {
        super();
    }

    // Εισαγωγή/ενημέρωση νέου key/value.
    @Override
    public int insert(int key, int value) throws RemoteException {

        if (key < 0 || value < 1) {
            System.out.println("Insert Error – Key: " + key + ", Value: " + value);
            return 0;
        }
        table.put(key, value);
        System.out.println("Inserted – Key: " + key + ", Value: " + value);
        return 1;
    }

    // Διαγραφή key.
    @Override
    public int delete(int key) throws RemoteException {
        if (key < 0) {
            System.out.println("Delete Error – Key: " + key);
            return 0;
        }

        Integer removed = table.remove(key); // Thread-safe remove
        if (removed == null) {
            System.out.println("Delete – Key not found: " + key);
            return 0;
        }

        System.out.println("Deleted – Key: " + key);
        return 1;
    }

    // Αναζήτηση value με βάση key.
    @Override
    public int search(int key) throws RemoteException {
        if (key < 0) {
            System.out.println("Search Error – Key: " + key);
            return 0;
        }

        Integer value = table.get(key);
        if (value == null) {
            System.out.println("Search – Key not found: " + key);
            return 0;
        }

        System.out.println("Search – Key: " + key + ", Value: " + value);
        return value;
    }
}
