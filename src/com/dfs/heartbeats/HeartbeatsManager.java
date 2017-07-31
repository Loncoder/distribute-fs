package com.dfs.heartbeats;

import java.rmi.RemoteException;
import java.util.Hashtable;
import java.util.Set;

public class HeartbeatsManager extends Thread {

    HeartbeatsListener listener;

    long period;

    Hashtable<Integer,HeartbeatsResponder> responders;

    boolean deattchOnFailure = true;

    public HeartbeatsManager(HeartbeatsListener listener,long period){
        this.listener = listener;
        this.period = period;
        responders = new Hashtable<>();
    }
    public void addachResponder(HeartbeatsResponder responder,int id){

        responders.put(id,responder);
    }

    public void deattachResponder(int id){
        responders.remove(id);
    }

    public boolean isDeattchOnFailure() {
        return deattchOnFailure;
    }

    public void setDeattchOnFailure(boolean deattchOnFailure) {
        this.deattchOnFailure = deattchOnFailure;
    }

    @Override
    public synchronized void start() {
        while (true){
            Set<Integer> keys = responders.keySet();
            for(Integer key : keys){
                HeartbeatsResponder responder = responders.get(key);
                boolean alive = false;
                try{
                    alive = responder.isAlive();
                }catch (RemoteException e){
                    e.printStackTrace();
                }
                if(!alive){
                    listener.onReponderFailure(responder,key);
                    if(deattchOnFailure)
                        responders.remove(key);
                }
            }
            try {
                sleep(period);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }
}
