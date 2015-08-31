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

package org.apache.ignite.internal.util.nio;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import junit.framework.TestCase;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Tests pure round trip time on network.
 */
public class GridRoundTripTest extends TestCase {
    /** Communication port. */
    public static final int PORT = 47600;

    /** Remote computer address. Change this field to run test. */
    public static final String HOSTNAME = "localhost";

    /**
     * @throws IOException If error occurs.
     * @throws InterruptedException If interrupted
     */
    public void testRunServer() throws IOException, InterruptedException {
        final ServerSocket sock = new ServerSocket();

        sock.bind(new InetSocketAddress("0.0.0.0", PORT));

        Thread runner = new Thread() {
            @Override public void run() {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        Socket accepted = sock.accept();

                        new EchoReader(accepted).start();
                    }
                }
                catch (IOException e) {
                    System.err.println("Accept thread failed: " + e.getMessage());
                }
                finally {
                    System.out.println("Server finished.");
                }
            }
        };

        runner.start();
        runner.join();
    }

    /**
     * Runs client test
     */
    @SuppressWarnings("InfiniteLoopStatement")
    public void testRunClient() {
        Socket sock = new Socket();

        OutputStream out = null;
        InputStream in = null;

        try {
            Random r = new Random();

            sock.connect(new InetSocketAddress(HOSTNAME, PORT));

            out = sock.getOutputStream();
            in = new BufferedInputStream(sock.getInputStream());

            while (true) {
                byte[] msg = createMessage(r.nextInt(1024) + 1);

                long start = System.currentTimeMillis();

                System.out.println(">>>>>>> [" + start + "] sending message, " + msg.length + " bytes");

                writeMessage(out, msg);

                byte[] resp = readMessage(in);

                if (resp.length != msg.length)
                    throw new IOException("Invalid response");

                long end = System.currentTimeMillis();

                System.out.println(">>>>>>> [" + end + "] response received, " + msg.length + " bytes");

                System.out.println("======= Response received within " + (end - start) + "ms\r\n");

                U.sleep(30);
            }
        }
        catch (Exception e) {
            System.out.println("Finishing test thread: " + e.getMessage());
        }
        finally {
            U.closeQuiet(out);
            U.closeQuiet(in);
            U.closeQuiet(sock);
        }
    }

    /**
     * Echo thread.
     */
    private static class EchoReader extends Thread {
        /** Client socket. */
        private Socket sock;

        /**
         * @param sock Accepted socket.
         */
        private EchoReader(Socket sock) {
            this.sock = sock;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("InfiniteLoopStatement")
        @Override public void run() {
            OutputStream out = null;
            InputStream in = null;

            try {
                out = sock.getOutputStream();
                in = new BufferedInputStream(sock.getInputStream());

                while (true) {
                    byte[] msg = readMessage(in);

                    System.out.println(">>>>>>> [" + System.currentTimeMillis() + "] packet received, " +
                        msg.length + " bytes");

                    System.out.println(">>>>>>> [" + System.currentTimeMillis() + "] sending response, " +
                        msg.length + " bytes");

                    writeMessage(out, msg);
                }
            }
            catch (Exception e) {
                System.out.println("Finishing client thread: " + e.getMessage());
            }
            finally {
                U.closeQuiet(in);
                U.closeQuiet(out);
                U.closeQuiet(sock);
            }
        }
    }

    /**
     * @param in Input stream to read from.
     * @return Read message.
     * @throws IOException If connection closed or packet was incorrect.
     */
    private static byte[] readMessage(InputStream in) throws IOException {
        ByteArrayOutputStream tmp = new ByteArrayOutputStream();

        for (int i = 0; i < 4; i++) {
            int symbol = in.read();

            if (symbol == -1)
                throw new IOException("Connection was closed.");

            tmp.write(symbol);
        }

        int length = U.bytesToInt(tmp.toByteArray(), 0);

        tmp.reset();

        for (int i = 0; i < length; i++) {
            int symbol = in.read();

            if (symbol == -1)
                throw new IOException("Connection was closed.");

            if ((byte)symbol != (byte)i)
                throw new IOException("Invalid packet: mismatch in position " + i);

            tmp.write(symbol);
        }

        return tmp.toByteArray();
    }

    /**
     * @param out Output stream to write to.
     * @param msg Message to write.
     * @throws IOException If error occurs.
     */
    private static void writeMessage(OutputStream out, byte[] msg) throws IOException {
        out.write(U.intToBytes(msg.length));
        out.write(msg);
    }

    /**
     * Creates message.
     *
     * @param len Message length.
     * @return Message bytes.
     */
    private static byte[] createMessage(int len) {
        byte[] res = new byte[len];

        for (int i = 0; i < len; i++)
            res[i] = (byte)i;

        return res;
    }
}