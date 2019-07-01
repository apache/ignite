/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.poweroff;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

/**
 * The listener application for the power off test.
 * The listener runs on a computer that stays on during the whole test.
 */
public class Listener implements Runnable {

    private volatile int maxValue;

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws IOException {
        new Listener().test(args);
    }

    private void test(String... args) throws IOException {
        int port = 9099;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-port")) {
                port = Integer.parseInt(args[++i]);
            }
        }
        listen(port);
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(10000);
            } catch (Exception e) {
                // ignore
            }
            System.out.println("Max=" + maxValue);
        }
    }

    private void listen(int port) throws IOException {
        new Thread(this).start();
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Listening on " + serverSocket.toString());
        long time;
        maxValue = 0;
        while (true) {
            Socket socket = serverSocket.accept();
            DataInputStream in = new DataInputStream(socket.getInputStream());
            System.out.println("Connected");
            time = System.nanoTime();
            try {
                while (true) {
                    int value = in.readInt();
                    if (value < 0) {
                        break;
                    }
                    maxValue = Math.max(maxValue, value);
                }
            } catch (IOException e) {
                System.out.println("Closed with Exception: " + e);
            }
            time = System.nanoTime() - time;
            int operationsPerSecond = (int) (TimeUnit.SECONDS.toNanos(1) * maxValue / time);
            System.out.println("Max=" + maxValue +
                    " operations/sec=" + operationsPerSecond);
        }
    }

}
