package com.dfs.server;

public class Transaction {

    public static final int COMMITED = 10;
    public static final int STARTED = 20;
    public static final int ABORTED = 30;

    private String fileName ;
    private int state;
    private long id;
    private long lastEdited;

    public Transaction(String fileName, int state, long id, long lastEdited) {
        this.fileName = fileName;
        this.state = state;
        this.id = id;
        this.lastEdited = lastEdited;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "time="+ System.currentTimeMillis()+
                "fileName='" + fileName + '\'' +
                ", state=" + state +
                ", id=" + id +
                ", lastEdited=" + lastEdited +
                '}';
    }

    public String getFileName() {
        return fileName;
    }

    public int getState() {
        return state;
    }

    public long getId() {
        return id;
    }

    public long getLastEdited() {
        return lastEdited;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void setState(int state) {
        this.state = state;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setLastEdited(long lastEdited) {
        this.lastEdited = lastEdited;
    }
}
