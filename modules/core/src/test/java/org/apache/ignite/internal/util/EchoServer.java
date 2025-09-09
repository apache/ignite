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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A simple TCP server that echoes back every byte that it receives. Can be used when some protocol-neutral property
 * of a TCP client (like socket closure handling) needs to be tested.
 *
 * This server must be operated (closed, address obtained, etc) from the same thread in which it was started.
 */
class EchoServer implements AutoCloseable {
    /***/
    private final int port;

    /***/
    private final ExecutorService acceptorExecutor = Executors.newSingleThreadExecutor();

    /***/
    private final ExecutorService workersExecutor = Executors.newCachedThreadPool();

    /***/
    private ServerSocket serverSocket;

    /***/
    private volatile boolean running;

    /***/
    EchoServer(int port) {
        this.port = port;
    }

    /***/
    public void start() throws IOException {
        running = true;

        serverSocket = new ServerSocket(port);

        acceptorExecutor.submit(new Acceptor());
    }

    /***/
    public void stop() throws IOException {
        assert running : "Not started yet";

        running = false;

        serverSocket.close();

        IgniteUtils.shutdownNow(getClass(), acceptorExecutor, null);
        IgniteUtils.shutdownNow(getClass(), workersExecutor, null);
    }

    /***/
    public SocketAddress localSocketAddress() {
        assert serverSocket != null;

        return serverSocket.getLocalSocketAddress();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        stop();
    }

    /***/
    private class Acceptor implements Runnable {
        /** {@inheritDoc} */
        @Override public void run() {
            while (running) {
                Socket sock = acceptConnection();
                workersExecutor.submit(new Worker(sock));
            }
        }

        /***/
        private Socket acceptConnection() {
            try {
                return serverSocket.accept();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /***/
    private class Worker implements Runnable {
        /***/
        private final Socket socket;

        /***/
        private Worker(Socket socket) {
            this.socket = socket;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try (Socket ignored = socket) {
                InputStream is = socket.getInputStream();
                OutputStream os = socket.getOutputStream();

                while (running) {
                    int ch = is.read();
                    if (ch < 0) {
                        break;
                    }
                    os.write(ch);
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
