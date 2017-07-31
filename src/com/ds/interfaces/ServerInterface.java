package com.ds.interfaces;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ServerInterface extends Remote{

    final static int ACK= 100;

    final static int ACK_RSND = 101;

    /**
     * Invalid transaction ID . Sent by the server if the client had sent a message that included an invalid transaction
     * ID, i.e., transaction ID that the server does not remember
     * */
    final static int INVALID_TRANSACTION_ID = 201;

    final static int INVALID_OPERATION = 202;

    final static String DFSERVER_UNIQUE_NAME = "dfs_name";

    final static String DFS_SECONDARY_SERVER_UNIQUE_NAME = "dsf_secondary_name";

    final static int SERVER_PROT = 5555;

    public FileContents read(String fileName) throws FileNotFoundException,
            IOException, RemoteException;

    public long newTxn(String fileName) throws RemoteException, IOException;

    public int write(long txnID, long msgSeqNum, byte[] data)
            throws RemoteException, IOException;

    public int commit(long txnID, long numOfMsgs)
            throws MessageNotFoundException, RemoteException;

    public int abort(long txnID) throws RemoteException;

    public boolean registerClient(ClientInterface client)
            throws RemoteException;
    public boolean unregisterClient(ClientInterface client)
            throws RemoteException;
}
