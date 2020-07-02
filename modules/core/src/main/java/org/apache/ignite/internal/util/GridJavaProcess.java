/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.lang.GridAbsClosure;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper class around the {@link Process} suited to run any Java class as separate java process.
 * <p>
 * This launcher supports simple interchange-with-a-process protocol to talk (in fact, listen) to process.
 * For the moment the only message in protocol is run process PID. Class-to-run should print it's PID
 * prefixed with {@code #PID_MSG_PREFIX} to tell the GridJavaProcess it's PID.
 * <p>
 * Protocol transport is any of (or both) <i>system.out</i> and/or <i>system.err</i>,
 * so any protocol message should be printed in the class-to-run.
 * <p>
 * NOTE 1: For the moment inner class running is not supported.
 * <p>
 * NOTE 2: This util class should work fine on Linux, Mac OS and Windows.
 */
public final class GridJavaProcess {
    /** Internal protocol message prefix saying that the next text in the outputted line is pid. */
    public static final String PID_MSG_PREFIX = "my_pid_is:";

    /** Logger */
    private IgniteLogger log;

    /** Wrapped system process. */
    private Process proc;

    /** Pid of wrapped process. Made as array to be changeable in nested static class. */
    private volatile String pid = "-1";

    /** system.out stream grabber for process in which user class is running. */
    private ProcessStreamGrabber osGrabber;

    /** system.err stream grabber for process in which user class is running. */
    private ProcessStreamGrabber esGrabber;

    /** Closure to be called when process termination is detected. */
    private GridAbsClosure procKilledC;

    /**
     * Private constructor to promote factory method usage.
     */
    private GridJavaProcess() {
        // No-op
    }

    /**
     * Executes main() method of the given class in a separate system process.
     *
     * @param cls Class with main() method to be run.
     * @param params main() method parameters.
     * @param printC Optional closure to be called each time wrapped process prints line to system.out or system.err.
     * @param procKilledC Optional closure to be called when process termination is detected.
     * @param log Log to use.
     * @return Wrapper around {@link Process}
     * @throws Exception If any problem occurred.
     */
    public static GridJavaProcess exec(Class cls, String params, @Nullable IgniteLogger log,
        @Nullable IgniteInClosure<String> printC, @Nullable GridAbsClosure procKilledC) throws Exception {
        return exec(cls.getCanonicalName(), params, log, printC, procKilledC, null, null, null);
    }

    /**
     * Executes main() method of the given class in a separate system process.
     *
     * @param cls Class with main() method to be run.
     * @param params main() method parameters.
     * @param printC Optional closure to be called each time wrapped process prints line to system.out or system.err.
     * @param procKilledC Optional closure to be called when process termination is detected.
     * @param log Log to use.
     * @param jvmArgs JVM arguments to use.
     * @param cp Additional classpath.
     * @return Wrapper around {@link Process}
     * @throws Exception If any problem occurred.
     */
    public static GridJavaProcess exec(Class cls, String params, @Nullable IgniteLogger log,
        @Nullable IgniteInClosure<String> printC, @Nullable GridAbsClosure procKilledC,
        @Nullable Collection<String> jvmArgs, @Nullable String cp) throws Exception {
        return exec(cls.getCanonicalName(), params, log, printC, procKilledC, null, jvmArgs, cp);
    }

    /**
     * Executes main() method of the given class in a separate system process.
     *
     * @param clsName Class with main() method to be run.
     * @param params main() method parameters.
     * @param log Log to use.
     * @param printC Optional closure to be called each time wrapped process prints line to system.out or system.err.
     * @param procKilledC Optional closure to be called when process termination is detected.
     * @param javaHome Java home location. The process will be started under given JVM.
     * @param jvmArgs JVM arguments to use.
     * @param cp Additional classpath.
     * @return Wrapper around {@link Process}
     * @throws Exception If any problem occurred.
     */
    public static GridJavaProcess exec(String clsName, String params, @Nullable IgniteLogger log,
        @Nullable IgniteInClosure<String> printC, @Nullable GridAbsClosure procKilledC,
        @Nullable String javaHome, @Nullable Collection<String> jvmArgs, @Nullable String cp) throws Exception {
        GridJavaProcess gjProc = new GridJavaProcess();

        gjProc.log = log;
        gjProc.procKilledC = procKilledC;

        List<String> procParams = params == null || params.isEmpty() ?
            Collections.<String>emptyList() : Arrays.asList(params.split(" "));

        List<String> procCommands = new ArrayList<>();

        String javaBin = (javaHome == null ? System.getProperty("java.home") : javaHome) +
            File.separator + "bin" + File.separator + "java";

        procCommands.add(javaBin);
        procCommands.addAll(jvmArgs == null ? U.jvmArgs() : jvmArgs);

        if (jvmArgs == null || (!jvmArgs.contains("-cp") && !jvmArgs.contains("-classpath"))) {
            String classpath = System.getProperty("java.class.path");

            String sfcp = System.getProperty("surefire.test.class.path");

            if (sfcp != null)
                classpath += System.getProperty("path.separator") + sfcp;

            if (cp != null)
                classpath += System.getProperty("path.separator") + cp;

            procCommands.add("-cp");
            procCommands.add(classpath);
        }

        procCommands.add(clsName);
        procCommands.addAll(procParams);

        ProcessBuilder builder = new ProcessBuilder(procCommands);

        builder.redirectErrorStream(true);

        Process proc = builder.start();

        gjProc.osGrabber = gjProc.new ProcessStreamGrabber(proc.getInputStream(), printC);
        gjProc.esGrabber = gjProc.new ProcessStreamGrabber(proc.getErrorStream(), printC);

        gjProc.osGrabber.start();
        gjProc.esGrabber.start();

        gjProc.proc = proc;

        return gjProc;
    }

    /**
     * Kills the java process.
     *
     * @throws Exception If any problem occurred.
     */
    public void kill() throws Exception {
        Process killProc = U.isWindows() ?
            Runtime.getRuntime().exec(new String[] {"taskkill", "/pid", pid, "/f", "/t"}) :
            Runtime.getRuntime().exec(new String[] {"kill", "-9", pid});

        killProc.waitFor();

        int exitVal = killProc.exitValue();

        if (exitVal != 0 && log.isInfoEnabled())
            log.info(String.format("Abnormal exit value of %s for pid %s", exitVal, pid));

        if (procKilledC != null)
            procKilledC.apply();

        U.interrupt(osGrabber);
        U.interrupt(esGrabber);

        U.join(osGrabber, log);
        U.join(esGrabber, log);
    }

    /**
     * Kills process using {@link Process#destroy()}.
     */
    public void killProcess() {
        proc.destroy();

        if (procKilledC != null)
            procKilledC.apply();

        U.interrupt(osGrabber);
        U.interrupt(esGrabber);

        U.join(osGrabber, log);
        U.join(esGrabber, log);
    }

    /**
     * Returns pid of the java process.
     * Wrapped java class should print it's PID to system.out or system.err to make wrapper know about it.
     *
     * @return Pid of the java process or -1 if pid is unknown.
     */
    public int getPid() {
        return Integer.valueOf(pid);
    }

    /**
     * Exposes wrapped java Process.
     *
     * @return Wrapped java process.
     */
    public Process getProcess() {
        return proc;
    }

    /**
     * Class which grabs sys.err and sys.out of the running process in separate thread
     * and implements the interchange-with-a-process protocol.
     */
    private class ProcessStreamGrabber extends Thread {
        /** Stream to grab. */
        private final InputStream streamToGrab;

        /** Closure to be called when process termination is detected. */
        private final IgniteInClosure<String> printC;

        /**
         * Creates the ProcessStreamGrabber bounded to the given Process.
         *
         * @param streamToGrab Stream to grab.
         * @param printC Optional closure to be called each time wrapped process prints line to system.out or system.err.
         */
        ProcessStreamGrabber(InputStream streamToGrab, @Nullable IgniteInClosure<String> printC) {
            this.streamToGrab = streamToGrab;
            this.printC = printC;
        }

        /**
         * Starts the ProcessStreamGrabber.
         */
        @Override public void run() {
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(streamToGrab));

                String line;

                while ((line = br.readLine()) != null && !isInterrupted()) {
                    if (line.startsWith(PID_MSG_PREFIX))
                        pid = line.substring(PID_MSG_PREFIX.length());
                    else
                        if (printC != null)
                            printC.apply(line);
                }
            }
            catch (IOException e) {
                U.error(log, "Caught IOException while grabbing stream", e);

                try {
                    // Check if process is still alive.
                    proc.exitValue();

                    if (procKilledC != null)
                        procKilledC.apply();
                }
                catch (IllegalThreadStateException e1) {
                    if (!interrupted())
                        U.error(log, "Failed to get exit value from process.", e1);
                }
            }
        }

        /**
         * Interrupts a thread and closes process streams.
         */
        @Override public void interrupt() {
            super.interrupt();

            // Close all Process streams to free allocated resources, see http://kylecartmell.com/?p=9.
            U.closeQuiet(proc.getErrorStream());
            U.closeQuiet(proc.getInputStream());
            U.closeQuiet(proc.getOutputStream());
        }
    }
}
