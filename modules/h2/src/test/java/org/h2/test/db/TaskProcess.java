/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import org.h2.test.utils.SelfDestructor;
import org.h2.util.StringUtils;
import org.h2.util.Task;

/**
 * A task that is run as an external process. This class communicates over
 * standard input / output with the process. The standard error stream of the
 * process is directly send to the standard error stream of this process.
 */
public class TaskProcess {
    private final TaskDef taskDef;
    private Process process;
    private BufferedReader reader;
    private BufferedWriter writer;

    /**
     * Construct a new task process. The process is not started yet.
     *
     * @param taskDef the task
     */
    public TaskProcess(TaskDef taskDef) {
        this.taskDef = taskDef;
    }

    /**
     * Start the task with the given arguments.
     *
     * @param args the arguments, or null
     */
    public void start(String... args) {
        try {
            String selfDestruct = SelfDestructor.getPropertyString(60);
            ArrayList<String> list = new ArrayList<>();
            list.add("java");
            list.add(selfDestruct);
            list.add("-cp");
            list.add("bin" + File.pathSeparator + ".");
            list.add(TaskDef.class.getName());
            list.add(taskDef.getClass().getName());
            if (args != null && args.length > 0) {
                list.addAll(Arrays.asList(args));
            }
            String[] procDef = list.toArray(new String[0]);
            process = Runtime.getRuntime().exec(procDef);
            copyInThread(process.getErrorStream(), System.err);
            reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
            String line = reader.readLine();
            if (line == null) {
                throw new RuntimeException(
                        "No reply from process, command: " +
                        StringUtils.arrayCombine(procDef, ' '));
            } else if (line.startsWith("running")) {
            } else if (line.startsWith("init error")) {
                throw new RuntimeException(line);
            }
        } catch (Throwable t) {
            throw new RuntimeException("Error starting task", t);
        }
    }

    private static void copyInThread(final InputStream in, final OutputStream out) {
        new Task() {
            @Override
            public void call() throws IOException {
                while (true) {
                    int x = in.read();
                    if (x < 0) {
                        return;
                    }
                    if (out != null) {
                        out.write(x);
                    }
                }
            }
        }.execute();
    }

    /**
     * Receive a message from the process over the standard output.
     *
     * @return the message
     */
    public String receive() {
        try {
            return reader.readLine();
        } catch (IOException e) {
            throw new RuntimeException("Error reading", e);
        }
    }

    /**
     * Send a message to the process over the standard input.
     *
     * @param message the message
     */
    public void send(String message) {
        try {
            writer.write(message + "\n");
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException("Error writing " + message, e);
        }
    }

    /**
     * Kill the process if it still runs.
     */
    public void destroy() {
        process.destroy();
    }

}
