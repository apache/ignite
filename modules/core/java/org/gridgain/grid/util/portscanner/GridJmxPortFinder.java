/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.portscanner;

import java.io.*;
import java.net.*;
import java.nio.channels.*;

/**
 * GridGain port scanner.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridJmxPortFinder {
    /** Minimum port number */
    private static final int MIN_PORT = 49112;

    /** Maximum port number */
    private static final int MAX_PORT = 65535;

    /**
     * Private constructor
     */
    private GridJmxPortFinder() {
        // No-op.
    }

    /**
     * Makes a search of available port. Start port is taken from temp file, it is
     * then replaced with newly found port.
     *
     * @param args Program arguments.
     * @throws IOException In case of error while reading or writing to file.
     */
    public static void main(String[] args) throws IOException {
        try {
            InetAddress.getLocalHost();
        }
        catch (UnknownHostException ignored) {
            // If the above call fails than JMX wouldn't start.
            // Do not return anything to signal inability to run JMX.
            return;
        }

        int port;

        RandomAccessFile ra = null;
        FileLock lock = null;

        try {
            File file = new File(System.getProperty("java.io.tmpdir"), "gridgain.lastport.tmp");

            file.setReadable(true, false);
            file.setWritable(true, false);

            ra = new RandomAccessFile(file, "rw");

            lock = ra.getChannel().lock();

            ra.seek(0);

            String startPortStr = ra.readLine();

            int startPort;

            if (startPortStr != null && !startPortStr.isEmpty()) {
                startPort = Integer.valueOf(startPortStr) + 1;

                if (startPort > MAX_PORT)
                    startPort = MIN_PORT;
            }
            else
                startPort = MIN_PORT;

            port = findPort(startPort);

            ra.setLength(0);

            ra.writeBytes(String.valueOf(port));
        }
        finally {
            if (lock != null)
                lock.release();

            if (ra != null)
                ra.close();
        }

        // Ack the port for others to read...
        System.out.println(port);
    }

    /**
     * Finds first available port beginning from start port trying given number of ports.
     *
     * @param startPort Start Port number.
     * @param range Number of ports to try. {@code 1} means try just the given number.
     * @return Available port number, or 0 if no available port found.
     */
    public static int findPort(int startPort, int range) {
        for (int port = startPort; port < startPort + range; port ++) {
            if (isAvailable(port))
                return port;
        }

        return 0;
    }

    /**
     * Finds first available port beginning from start port up to {@link GridJmxPortFinder#MAX_PORT}.
     *
     * @param startPort Start Port number.
     * @return Available port number, or 0 if no available port found.
     */
    private static int findPort(int startPort) {
        return findPort(startPort, MAX_PORT - startPort + 1);
    }

    /**
     * Checks whether port is available.
     *
     * @param port Port number.
     * @return {@code true} if port is available
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
