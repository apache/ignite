/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.h2.test.utils.SelfDestructor;

/**
 * A task that can be run as a separate process.
 */
public abstract class TaskDef {

    /**
     * Run the class. This method is called by the task framework, and should
     * not be called directly from the application.
     *
     * @param args the command line arguments
     */
    public static void main(String... args) {
        SelfDestructor.startCountdown(60);
        TaskDef task;
        try {
            String className = args[0];
            task = (TaskDef) Class.forName(className).newInstance();
            System.out.println("running");
        } catch (Throwable t) {
            System.out.println("init error: " + t);
            t.printStackTrace();
            return;
        }
        try {
            task.run(Arrays.copyOf(args, args.length - 1));
        } catch (Throwable t) {
            System.out.println("error: " + t);
            t.printStackTrace();
        }
    }

    /**
     * Run the task.
     *
     * @param args the command line arguments
     */
    abstract void run(String... args) throws Exception;

    /**
     * Receive a message from the process over the standard output.
     *
     * @return the message
     */
    protected String receive() {
        try {
            return new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (IOException e) {
            throw new RuntimeException("Error reading from input", e);
        }
    }

    /**
     * Send a message to the process over the standard input.
     *
     * @param message the message
     */
    protected void send(String message) {
        System.out.println(message);
        System.out.flush();
    }

}
