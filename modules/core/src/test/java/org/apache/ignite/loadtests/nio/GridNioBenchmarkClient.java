/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.loadtests.nio;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.internal.util.typedef.X;

/**
 *
 */
public class GridNioBenchmarkClient {
    /** */
    private static final int INITIAL_PACKET_SIZE = 65536;

    /** */
    private static final byte[] INITIAL_PACKET = new byte[INITIAL_PACKET_SIZE];

    /**
     *
     */
    static {
        Random r = new Random();

        for (int i = 0; i < INITIAL_PACKET_SIZE; i++)
            r.nextBytes(INITIAL_PACKET);
    }

    /** */
    private final int connCnt;

    /** */
    private final String host;

    /** */
    private final int port;

    /** */
    private final ExecutorService exec;

    /** */
    private final byte[] buf = new byte[(int)(65536*1.5)];

    /**
     * @param connCnt Connections count.
     * @param host Host.
     * @param port Port.
     */
    public GridNioBenchmarkClient(int connCnt, String host, int port) {
        this.connCnt = connCnt;
        this.host = host;
        this.port = port;

        exec = Executors.newFixedThreadPool(connCnt);
    }

    /**
     * Runs single benchamark configuration.
     *
     * @throws IOException If connection failed.
     * @throws InterruptedException If benchmark was interrupted.
     */
    public void run() throws IOException, InterruptedException {
        for (int i = 0; i < connCnt; i++)
            exec.execute(new ClientThread());

        Thread.sleep(5*60*1000);

        exec.shutdownNow();
    }

    /**
     * Runs set of tests.
     *
     * @param args Command line arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            X.println("Usage: " + GridNioBenchmarkClient.class.getSimpleName() + " <connections count> <host> <port>");

            return;
        }

        final int connCnt = Integer.parseInt(args[0]);
        final String host = args[1];
        final int port = Integer.parseInt(args[2]);

        new GridNioBenchmarkClient(connCnt, host, port).run();
    }

    /**
     * Test thread.
     */
    private class ClientThread implements Runnable {
        /** {@inheritDoc} */
        @Override public void run() {
            Socket s = new Socket();

            try {
                s.connect(new InetSocketAddress(host, port));

                InputStream in = s.getInputStream();
                OutputStream out = s.getOutputStream();

                out.write(INITIAL_PACKET);

                for (int i = 0; i < 1000000; i++)
                    doIteration(in, out);

                long bytes = 0;

                long start = System.currentTimeMillis();

                while (!Thread.interrupted())
                    bytes += doIteration(in, out);

                long duration = System.currentTimeMillis() - start;

                long mb = bytes/1048576;

                X.println("Thread finished [MB=" + bytes/1048576 + ", MB/s=" + ((double)mb)*1000/duration + "]");
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * Performs single test iterations.
         *
         * @param in Stream to read data.
         * @param out Stream to write data.
         * @return Echoed bytes count.
         * @throws IOException If failed.
         */
        @SuppressWarnings("CallToThreadYield")
        private long doIteration(InputStream in, OutputStream out) throws IOException {
            int read = in.read(buf, 0, in.available());

            if (read == 0)
                Thread.yield();

            out.write(buf, 0, read);

            return read;
        }
    }
}
