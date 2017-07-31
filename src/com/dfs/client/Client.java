package com.dfs.client;

import com.ds.interfaces.ClientInterface;
import com.ds.interfaces.MessageNotFoundException;
import com.ds.interfaces.ServerInterface;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;

public class Client implements ClientInterface {
    private String auth_token;
    private String hostIP;
    ServerInterface server;
    int port;

    public Client(String serverHostIP, int port) throws RemoteException,
            NotBoundException {
        this.hostIP = serverHostIP;
        this.port = port;

        Registry reg = LocateRegistry.getRegistry(hostIP, port);
        server = (ServerInterface) reg
                .lookup(ServerInterface.DFSERVER_UNIQUE_NAME);

        UnicastRemoteObject.exportObject(this, 5412);
        server.registerClient(this);
    }

    @Override
    public void updateServerIP(String ip, int port) {
        this.hostIP = ip;
        this.port = port;

        try {
            Registry reg = LocateRegistry.getRegistry(hostIP, port);
            server = (ServerInterface) reg
                    .lookup(ServerInterface.DFSERVER_UNIQUE_NAME);
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (AccessException e) {
            e.printStackTrace();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
    public void setAuthenticationToken(String auth_token)
            throws RemoteException {
        this.auth_token = auth_token;
    }

    @Override
    public String getAuthenticationToken() throws RemoteException {
        return this.auth_token;
    }

    public String read(String fileName) throws FileNotFoundException,
            RemoteException, IOException {
        byte[] content = server.read(fileName).get();
        return new String(content);
    }

    public long write(String fileName, String content)
            throws RemoteException, IOException, MessageNotFoundException {
        long txid = server.newTxn(fileName);
        server.write(txid, 1, content.getBytes());
        return txid;
    }
    public static void main(String[] args) throws NotBoundException,
            FileNotFoundException, IOException, MessageNotFoundException, InterruptedException {
        Client c = new Client("localhost", 5555); // 5892
        long txid = c.server.newTxn("mashary.txt");

        c.server.write(txid, 1, "This is a test1\n".getBytes());
        c.server.write(txid, 2, "This is a test2\n".getBytes());

        Thread.sleep(10000);

        c.server.write(txid, 3, "This is a test3\n".getBytes());
        c.server.write(txid, 4, "This is a test4\n".getBytes());

        try {
            c.server.commit(txid, 4);
        } catch (MessageNotFoundException e) {
            System.out.println(Arrays.toString(e.getMsgNum()));
        }

        System.out.println(new String(c.server.read("mashary.txt").get()));

    }

}
