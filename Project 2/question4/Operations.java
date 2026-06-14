package question4;

import java.rmi.Remote;
import java.rmi.RemoteException;

//RMI Interface ορισμός μεθόδων.

public interface Operations extends Remote {


    int insert(int key, int value) throws RemoteException;

    int delete(int key) throws RemoteException;

    int search(int key) throws RemoteException;
}
