package question3;

public class Main {
    // Ορισμός αριθμού server, consumer και producer που θα ξεκινήσουν
    private static final int SERVERS = 2;
    private static final int CONSUMERS = 1;
    private static final int PRODUCERS = 1;

    public static void main(String[] args) {
        String[] hosts = new String[SERVERS];
        int[] ports = new int[SERVERS];

        // Αρχικοποίηση IP και θυρών για τους servers και εκκίνηση τους
        for (int i = 0; i < SERVERS; i++) {
            hosts[i] = "127.0.0.1";           // localhost IP
            ports[i] = 9000 + i;              // κάθε server σε διαφορετική πόρτα
            new ServerStarter("S" + i, ports[i]).start(); // εκκίνηση server σε νέο νήμα
        }

        // Εκκίνηση των consumers (καταναλωτών)
        for (int i = 0; i < CONSUMERS; i++) {
            new ConsumerStarter("C" + i, hosts, ports).start(); // νέος consumer thread
        }

        // Εκκίνηση των producers (παραγωγών)
        for (int i = 0; i < PRODUCERS; i++) {
            new ProducerStarter("P" + i, hosts, ports).start(); // νέος producer thread
        }
    }

    // Εσωτερική κλάση για εκκίνηση ενός server σε ξεχωριστό νήμα
    private static class ServerStarter extends Thread {
        String name;
        int port;

        public ServerStarter(String name, int port) {
            this.name = name;
            this.port = port;
        }

        @Override
        public void run() {
            new Server(name, port); // δημιουργία αντικειμένου Server
        }
    }

    // Εσωτερική κλάση για εκκίνηση ενός consumer
    private static class ConsumerStarter extends Thread {
        String name;
        String[] hosts;
        int[] ports;

        public ConsumerStarter(String name, String[] hosts, int[] ports) {
            this.name = name;
            this.hosts = hosts;
            this.ports = ports;
        }

        @Override
        public void run() {
            new Consumer(name, hosts, ports); // δημιουργία αντικειμένου Consumer
        }
    }

    // Εσωτερική κλάση για εκκίνηση ενός producer
    private static class ProducerStarter extends Thread {
        String name;
        String[] hosts;
        int[] ports;

        public ProducerStarter(String name, String[] hosts, int[] ports) {
            this.name = name;
            this.hosts = hosts;
            this.ports = ports;
        }

        @Override
        public void run() {
            new Producer(name, hosts, ports); // δημιουργία αντικειμένου Producer
        }
    }
}
