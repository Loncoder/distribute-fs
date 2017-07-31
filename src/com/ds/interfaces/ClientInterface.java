package com.ds.interfaces;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ClientInterface extends Remote {

    void updateServerIP(String ip,int port);

    void setAuthenticationToken(String auth_token) throws RemoteException;

    String getAuthenticationToken() throws RemoteException;
}
