package com.dfs.server;

import com.ds.interfaces.*;

import java.io.*;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;

public class ReplicaServer implements ReplicaServerInterface {

    String directory_path = System.getProperty("user.home")+"/dfs/";
    String cache_path = directory_path + "cache/";

    public ReplicaServer(String host, String directory_path){
        if(directory_path != null){
            this.directory_path = directory_path+"/";
            this.cache_path = directory_path + "cache/";
        }
        new File(directory_path).mkdir();
        new File(cache_path).mkdir();

    }
    @Override
    public FileContents read(String fileName) throws FileNotFoundException, IOException, RemoteException {

        FileInputStream inputStream = new FileInputStream(new File(directory_path+fileName));
        byte [] buffer = new byte[FileContents.BUFFER_SIZE];
        int contentlength = inputStream.read(buffer);
        inputStream.close();

        byte[] content = new byte[contentlength];
        System.arraycopy(buffer,0,content,0,contentlength);
        return new FileContents(content);
    }

    @Override
    public long newTxn(String fileName) throws RemoteException, IOException {
        throw  new UnsupportedOperationException();
    }

    @Override
    public int write(long txnID, long msgSeqNum, byte[] data) throws RemoteException, IOException {
        File cacheFile = new File(cache_path+txnID+"_"+msgSeqNum);
        cacheFile.createNewFile();

        FileOutputStream outputStream = new FileOutputStream(cacheFile);
        outputStream.write(data);
        outputStream.flush();
        outputStream.close();
        return ACK;
    }



    @Override
    public int commit(long txnID, long numOfMsgs, String filename) throws MessageNotFoundException, RemoteException {
        File[] cachedFiles = new File(cache_path).listFiles(new CacheFilesFilter(txnID));
        // check if there are unreceived messages and report them to the client
        if (cachedFiles.length < numOfMsgs) {
            long[] msgsIDs = new long[cachedFiles.length];

            // convert msgsID to array of Long
            for (int i = 0; i < msgsIDs.length; i++) {
                String fname = cachedFiles[(int) i].getName();
                msgsIDs[i] = Long.parseLong(fname.substring(fname.indexOf('_') + 1));
            }

            // prepare exception to be thrown
            MessageNotFoundException exception = new MessageNotFoundException();
            exception.setMsgNum(findLostMessagesIDs(msgsIDs, numOfMsgs));

            throw exception;
        }
        try {

            // create new file if it is not exist yet.
            File fout = new File(directory_path + filename);
            fout.createNewFile();

            FileOutputStream outsream = new FileOutputStream(fout, true);

            byte[] buffer = new byte[FileContents.BUFFER_SIZE];
            for (int i = 1; i <= numOfMsgs; i++) {
                FileInputStream instream = new FileInputStream(new File(cache_path + txnID + '_' + i));

                int len = 0;
                while ((len = instream.read(buffer)) != -1) {
                    outsream.write(buffer, 0, len);
                }

                instream.close();
            }

            // flush and close file output stream
            outsream.flush();
            outsream.close();


        }catch (IOException e){
            e.printStackTrace();
        }
        clearCacheFiles(txnID);
        return ACK;
    }
    private void clearCacheFiles(long txnID){
        File [] cacheFiles = new File(cache_path).listFiles(new CacheFilesFilter(txnID));
        for(File file : cacheFiles){
            file.delete();
        }
    }

    private int[] findLostMessagesIDs(long[] msgsIDs, long numOfMsgs) {
        Arrays.sort(msgsIDs);

        int missedMessagesNumner = (int) numOfMsgs - msgsIDs.length;
        int[] missedMessages = new int[missedMessagesNumner];
        int mIndex = 0;

        if(msgsIDs[0] != 1){
            for (long j = 1; j < msgsIDs[0]; j++) {
                missedMessages[mIndex++] = (int) j;
            }
        }

        for (int i = 1; i < msgsIDs.length; i++) {
            if ((msgsIDs[i] - msgsIDs[i - 1]) != 1) {
                for (long j = msgsIDs[i - 1] + 1; j < msgsIDs[i]; j++) {
                    missedMessages[mIndex++] = (int) j;
                }
            }
        }

        if(msgsIDs[msgsIDs.length - 1] != numOfMsgs){
            for (long j = msgsIDs[msgsIDs.length - 1] + 1; j <= numOfMsgs; j++) {
                missedMessages[mIndex++] = (int) j;
            }
        }

        return missedMessages;
    }




    @Override
    public int abort(long txnID) throws RemoteException {
        clearCacheFiles(txnID);
        return ACK;
    }

    @Override
    public boolean registerClient(ClientInterface client) throws RemoteException {
         throw  new UnsupportedOperationException();
    }

    @Override
    public boolean unregisterClient(ClientInterface client) throws RemoteException {
        throw  new UnsupportedOperationException();
    }

    @Override
    public int commit(long txnID, long numOfMsgs) throws MessageNotFoundException, RemoteException {
        throw  new UnsupportedOperationException();

    }

    /**
     * FilenameFilter used to filter cached files for specific transaction
     * */
    class CacheFilesFilter implements FilenameFilter {
        long txnID = 0;

        public boolean accept(File dir, String name) {
            name = name.substring(0, name.indexOf('_'));
            return name.equals("" + txnID);
        }

        CacheFilesFilter(long txnID) {
            this.txnID = txnID;
        }
    }

    public void init(String name,int port) throws RemoteException,AlreadyBoundException,NotBoundException{

        Object mainServerExportedObject = UnicastRemoteObject.exportObject(this,port);
        ReplicaServerInterface serverStub = (ReplicaServerInterface) mainServerExportedObject;
        Registry registry = LocateRegistry.createRegistry(port);
        registry.rebind(name,serverStub);

//       完成slaver server之后，将slaver进行服务注册，方便用户进行调用。

    }

    public static void main(String[] args) throws AlreadyBoundException, RemoteException,NotBoundException {
        System.out.println("starting replica 1 ....");
        new ReplicaServer("localhost", "1").init("replica1", 5678);
        System.out.println("Done");



        System.out.println("starting replica 1 ....");
        new ReplicaServer("localhost", "2").init("replica2", 5679);
        System.out.println("Done");

    }

}
