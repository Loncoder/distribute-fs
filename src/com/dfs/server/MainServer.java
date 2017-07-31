package com.dfs.server;

import com.dfs.heartbeats.HeartbeatsResponder;
import com.dfs.log.Logger;
import com.ds.interfaces.*;
import com.sun.org.apache.regexp.internal.RE;

import java.io.*;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

public class MainServer implements ServerInterface, HeartbeatsResponder {
    String directory_path = System.getProperty("user.home") + "/dfs/";
    String log_path = directory_path + "log/";
    private ArrayList<ReplicaServerInfo> replicaservers = new ArrayList<>();

    private Hashtable<Long,Transaction> transactions = new Hashtable<>();

    private Hashtable<String, ClientInterface> clients = new Hashtable<String, ClientInterface>();

    private HashSet<String> fileLocks = new HashSet<String>();

    private Logger logger;
    private Random random;
    private static long idleTimeout = 6000;

    SecondaryServerInterface secondaryServer;

    public static final String MAIN_SERVER_HEARTBEAT_NAME = "main_server_responder";

    /**
     * Constructing MainServe object with main attributes, this is used when
     * running new MainServer instance from the secondary server when the
     * original main server is failed.
     * */
    public MainServer(Logger logger,
                      Hashtable<String, ClientInterface> clients,
                      Hashtable<Long, Transaction> transactions, String directoryPath) {

        this.logger = logger;
        this.directory_path = directoryPath;
        this.clients = clients;
        this.transactions = transactions;
        this.random = new Random(System.currentTimeMillis());
    }


    public MainServer(String secondaryServerHost, int secondaryServerPort,String directory_path)throws RemoteException,NotBoundException{
        if(directory_path!=null){
            this.directory_path = directory_path;
            this.log_path = log_path;
        }
        this.clients = new Hashtable<>();
        this.random = new Random(System.currentTimeMillis());
        if(secondaryServerHost!=null){
            Registry registry = LocateRegistry.getRegistry(secondaryServerHost,secondaryServerPort);
            secondaryServer = (SecondaryServerInterface) registry.lookup(DFS_SECONDARY_SERVER_UNIQUE_NAME);
        }
        new File(log_path).mkdir();
        this.logger = new Logger();
        this.logger.init(log_path+"log.txt");

    }
    ReplicaServerInterface getServer(ReplicaServerInfo replicaServerInfo) {
        Registry registry;
        try {
            registry = LocateRegistry.getRegistry(replicaServerInfo.hostName, replicaServerInfo.port);
            return (ReplicaServerInterface) registry.lookup(replicaServerInfo.uniqueName);
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public FileContents read(String fileName) throws FileNotFoundException, IOException, RemoteException {
        int idx = random.nextInt(replicaservers.size());
        ServerInterface rServer = (ServerInterface) getServer(replicaservers.get(idx));
        FileContents contents = rServer.read(fileName);
        long time = System.currentTimeMillis();
        logger.logReadFile(fileName,time);

        if(secondaryServer != null){
            secondaryServer.read(fileName,time);
        }
        return contents;
    }

    @Override
    public long newTxn(String fileName) throws RemoteException, IOException {

        long time = System.currentTimeMillis();
        long txnId = time;
        Transaction tx = new Transaction(fileName,Transaction.STARTED,txnId,time);
        logger.logTransaction(tx,time);
        if(secondaryServer!=null){
            secondaryServer.newTxn(fileName,txnId,time);
        }
        transactions.put(txnId, tx);
        return txnId;
    }

    @Override
    public int write(long txnID, long msgSeqNum, byte[] data) throws RemoteException, IOException {
        if(!transactions.containsKey(txnID)){
            return INVALID_TRANSACTION_ID;
        }
        if(transactions.get(txnID).getState() == Transaction.COMMITED){
            return INVALID_OPERATION;
        }
        for(ReplicaServerInfo name : replicaservers){
            ReplicaServerInterface server = (ReplicaServerInterface)getServer(name);
            if(server!=null){
                server.write(txnID,msgSeqNum,data);
            }
        }
        long time = System.currentTimeMillis();
        logger.logWriteRequest(txnID,msgSeqNum,data.length,time);
        if(secondaryServer!=null){
            secondaryServer.write(txnID,msgSeqNum,data.length,time);
        }
        return ACK;
    }

    private synchronized void grantFileLock(String fileName){
        System.out.println("trying to grant lock");
        while(fileLocks.contains(fileName)){
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        fileLocks.add(fileName);
        System.out.println("lock granted!");
    }

    private void releaseFileLock(String fileName){
        fileLocks.remove(fileName);
        System.out.println("lock released!");
    }

    @Override
    public int commit(long txnID, long numOfMsgs) throws MessageNotFoundException, RemoteException {
        // check if the transaction id is correct
        if (!transactions.containsKey(txnID)) {
            return INVALID_TRANSACTION_ID;
        }
        // check if the transaction has been already committed
        if (transactions.get(txnID).getState() == Transaction.COMMITED) {
            // the client me request resending the ack message
            return ACK;
        }

        Transaction tx = transactions.get(txnID);

        // granting lock on the file name
        grantFileLock(tx.getFileName());

        for (ReplicaServerInfo name : replicaservers) {
            ReplicaServerInterface server = (ReplicaServerInterface) getServer(name);

            if (server != null)
                server.commit(txnID, numOfMsgs, tx.getFileName());
        }

        // update transaction state and log it
        tx.setState(Transaction.COMMITED);
        long time = System.currentTimeMillis();
        logger.logTransaction(tx, time);

        // release file lock
        releaseFileLock(tx.getFileName());

        if (secondaryServer != null)
            secondaryServer.commit(txnID, tx.getFileName(), time);

        return ACK;

    }

    @Override
    public int abort(long txnID) throws RemoteException {

        if(!transactions.containsKey(txnID)){
            return INVALID_TRANSACTION_ID;
        }
        if(transactions.get(txnID).getState()==Transaction.COMMITED){
            return INVALID_OPERATION;
        }
        if(transactions.get(txnID).getState()==Transaction.ABORTED) {
            return ACK;
        }
        for(ReplicaServerInfo name : replicaservers){
            ReplicaServerInterface server = (ReplicaServerInterface)getServer(name);
            if(server!=null)
                server.abort(txnID);
        }
        long time = System.currentTimeMillis();
        transactions.get(txnID).setState(Transaction.ABORTED);
        logger.logTransaction(transactions.get(txnID),time);
        if(secondaryServer!=null)
            secondaryServer.abort(txnID,transactions.get(txnID).getFileName(),time);
        return ACK;
    }

    @Override
    public boolean registerClient(ClientInterface client) throws RemoteException {
        String auth_token = client.getAuthenticationToken();

        if (auth_token == null) {
            // generate new auth token
            auth_token = UUID.randomUUID().toString();
            client.setAuthenticationToken(auth_token);

            // add this new client to the list of authenticated clients
            this.clients.put(auth_token, client);

            if (secondaryServer != null)
                secondaryServer.registerClient(client, auth_token);

            return true;
        } else {
            if (clients.containsKey(auth_token))
                return true;
            else
                return false;
        }

    }

    @Override
    public boolean unregisterClient(ClientInterface client) throws RemoteException {
        String auth_token = client.getAuthenticationToken();

        if (auth_token == null) {
            // Unresisted client
            return false;
        } else {
            if (clients.containsKey(auth_token)) {
                // safely remove this client
                clients.remove(auth_token);

                if (secondaryServer != null)
                    secondaryServer.unregisterClient(client, auth_token);

                return true;
            } else {
                // unrecognized auth token, safely return false
                return false;
            }
        }

    }

    public void init(int port)throws AlreadyBoundException,IOException{
        Object mainServer = UnicastRemoteObject.exportObject(this,port);
        ServerInterface serverStub = (ServerInterface)mainServer;
        HeartbeatsResponder heartbeatsResponder = (HeartbeatsResponder)mainServer;
        Registry registry = LocateRegistry.createRegistry(port);
        registry.rebind(DFSERVER_UNIQUE_NAME,serverStub);
        registry.rebind(MAIN_SERVER_HEARTBEAT_NAME,heartbeatsResponder);

        transactionsTimeoutChecker.start();
        InputStreamReader convert = new InputStreamReader(new FileInputStream(new File("ReplicaServers")));
        BufferedReader in =  new BufferedReader(convert);
        String line = null;
        in.readLine();
        while ((line=in.readLine())!=null){
            replicaservers.add(new ReplicaServerInfo(line));
        }//

        //当mainServer完成之后，在服务中进行注册，方便调用。
        //master 根据注册信息，进行查找，并获取slave的对象引用

    }
    private Thread transactionsTimeoutChecker = new Thread(new Runnable() {

        @Override
        public void run() {
            while(true){
                long now = System.currentTimeMillis();
                Object[] keys = MainServer.this.transactions.keySet().toArray();
                for (Object key : keys) {
                    Transaction t = MainServer.this.transactions.get((Long)key);

                    // clean aborted and commited transactions from transaction hash table
                    if(t.getState() == Transaction.COMMITED || t.getState() == Transaction.ABORTED){
                        MainServer.this.transactions.remove(key);
                    }

                    // check transaction time and state
                    if((now - t.getLastEdited()) > idleTimeout){
                        try {
                            MainServer.this.abort((Long)key);
                        } catch (RemoteException e) {
                            e.printStackTrace();
                        }
                        MainServer.this.transactions.remove(key);
                    }
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    });


    @Override
    public boolean isAlive() throws RemoteException {
        return true;
    }

    class ReplicaServerInfo{
        String uniqueName;
        String hostName;
        int port;

        public ReplicaServerInfo(String info){
            StringTokenizer st = new StringTokenizer(info, "\t");
            hostName = st.nextToken();
            port = Integer.parseInt(st.nextToken());
            uniqueName = st.nextToken();
        }
    }
}
