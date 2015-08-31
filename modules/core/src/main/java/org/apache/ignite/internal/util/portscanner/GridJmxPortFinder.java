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

package org.apache.ignite.internal.util.portscanner;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.nio.channels.FileLock;
import org.apache.ignite.IgniteSystemProperties;

/**
 * Ignite port scanner.
 */
public class GridJmxPortFinder {
    /** Environment variable for overriding JMX port. */
    public static final String IGNITE_JMX_PORT = "IGNITE_JMX_PORT";

    /** Minimum port number. */
    private static final int MIN_PORT = 49112;

    /** Maximum port number. */
    private static final int MAX_PORT = 65535;

    /**
     * Private constructor.
     */
    private GridJmxPortFinder() {
        // No-op.
    }

    /**
     * Makes a search of available port. Start port is taken from temp file, it is
     * then replaced with newly found port.
     *
     * @param args Program arguments.
     */
    public static void main(String[] args) {
        try {
            InetAddress.getLocalHost();
        }
        catch (UnknownHostException ignored) {
            // If the above call fails than JMX wouldn't start.
            // Do not return anything to signal inability to run JMX.
            return;
        }

        int jmxPort = IgniteSystemProperties.getInteger(IGNITE_JMX_PORT, -1);

        if (jmxPort != -1) {
            System.out.println(jmxPort);

            return;
        }

        RandomAccessFile ra = null;
        FileLock lock = null;

        try {
            File file = new File(System.getProperty("java.io.tmpdir"), "ignite.lastport.tmp");

            file.setReadable(true, false);
            file.setWritable(true, false);

            ra = new RandomAccessFile(file, "rw");

            lock = ra.getChannel().lock();

            ra.seek(0);

            String startPortStr = ra.readLine();

            int startPort = MIN_PORT;

            if (startPortStr != null && !startPortStr.isEmpty()) {
                try {
                    startPort = Integer.valueOf(startPortStr) + 1;

                    if (startPort < MIN_PORT || startPort > MAX_PORT)
                        startPort = MIN_PORT;
                }
                catch (NumberFormatException ignored) {
                    // Ignore, just use default lower bound port.
                }
            }

            int port = findPort(startPort);

            ra.setLength(0);

            ra.writeBytes(String.valueOf(port));

            // Ack the port for others to read...
            System.out.println(port);
        }
        catch (IOException ignored) {
            // Do not return anything to signal inability to run JMX.
        }
        finally {
            if (lock != null)
                try {
                    lock.release();
                }
                catch (IOException ignored) {
                    // No-op.
                }

            if (ra != null)
                try {
                    ra.close();
                }
                catch (IOException ignored) {
                    // No-op.
                }
        }
    }

    /**
     * Finds first available port beginning from start port up to {@link GridJmxPortFinder#MAX_PORT}.
     *
     * @param startPort Start Port number.
     * @return Available port number, or 0 if no available port found.
     */
    private static int findPort(int startPort) {
        for (int port = startPort; port <= MAX_PORT; port++) {
            if (isAvailable(port))
                return port;
        }

        return 0;
    }

    /**
     * Checks whether port is available.
     *
     * @param port Port number.
     * @return {@code true} if port is available.
     */
    private static boolean isAvailable(int port) {
        ServerSocket sock = null;

        try {
            sock = new ServerSocket(port);

            return true;
        }
        catch (IOException ignored) {
            return false;
        }
        finally {
            if (sock != null) {
                try {
                    sock.close();
                }
                catch (IOException ignored) {
                    // No-op
                    // Could we leave it unavailable here? Possible "return false;".
                }
            }
        }
    }
}