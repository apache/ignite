/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.ftp.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import org.h2.store.fs.FileUtils;
import org.h2.util.IOUtils;

/**
 * The implementation of the data channel of the FTP server.
 */
public class FtpData extends Thread {

    private final FtpServer server;
    private final InetAddress address;
    private ServerSocket serverSocket;
    private volatile Socket socket;
    private final boolean active;
    private final int port;

    FtpData(FtpServer server, InetAddress address, ServerSocket serverSocket) {
        this.server = server;
        this.address = address;
        this.serverSocket = serverSocket;
        this.port = 0;
        this.active = false;
    }

    FtpData(FtpServer server, InetAddress address, int port) {
        this.server = server;
        this.address = address;
        this.port = port;
        this.active = true;
    }

    @Override
    public void run() {
        try {
            synchronized (this) {
                Socket s = serverSocket.accept();
                if (s.getInetAddress().equals(address)) {
                    server.trace("Data connected:" + s.getInetAddress() + " expected:" + address);
                    socket = s;
                    notifyAll();
                } else {
                    server.trace("Data REJECTED:" + s.getInetAddress() + " expected:" + address);
                    close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void connect() throws IOException {
        if (active) {
            socket = new Socket(address, port);
        } else {
            waitUntilConnected();
        }
    }

    private void waitUntilConnected() {
        while (serverSocket != null && socket == null) {
            try {
                wait();
            } catch (InterruptedException e) {
                // ignore
            }
        }
        server.trace("connected");
    }

    /**
     * Close the socket.
     */
    void close() {
        serverSocket = null;
        socket = null;
    }

    /**
     * Read a file from a client.
     *
     * @param fileName the target file name
     */
    synchronized void receive(String fileName) throws IOException {
        connect();
        try {
            InputStream in = socket.getInputStream();
            OutputStream out = FileUtils.newOutputStream(fileName, false);
            IOUtils.copy(in, out);
            out.close();
        } finally {
            socket.close();
        }
        server.trace("closed");
    }

    /**
     * Send a file to the client. This method waits until the client has
     * connected.
     *
     * @param fileName the source file name
     * @param skip the number of bytes to skip
     */
    synchronized void send(String fileName, long skip) throws IOException {
        connect();
        try {
            OutputStream out = socket.getOutputStream();
            InputStream in = FileUtils.newInputStream(fileName);
            IOUtils.skipFully(in, skip);
            IOUtils.copy(in, out);
            in.close();
        } finally {
            socket.close();
        }
        server.trace("closed");
    }

    /**
     * Wait until the client has connected, and then send the data to him.
     *
     * @param data the data to send
     */
    synchronized void send(byte[] data) throws IOException {
        connect();
        try {
            OutputStream out = socket.getOutputStream();
            out.write(data);
        } finally {
            socket.close();
        }
        server.trace("closed");
    }

}
