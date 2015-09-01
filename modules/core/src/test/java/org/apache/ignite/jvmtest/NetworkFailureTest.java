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

package org.apache.ignite.jvmtest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.swing.JOptionPane;
import junit.framework.TestCase;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class NetworkFailureTest extends TestCase {
    /**
     * @throws Exception If failed.
     */
    public void testNetworkFailure() throws Exception {
        final AtomicBoolean done = new AtomicBoolean();

        final InetAddress addr = InetAddress.getByName("192.168.0.100");

        IgniteInternalFuture<?> fut1 = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    ServerSocket srvSock = null;
                    Socket sock = null;

                    try {
                        srvSock = new ServerSocket(60000, 0, addr);

                        sock = srvSock.accept();

                        X.println("Socket [timeout=" + sock.getSoTimeout() + ", linger=" + sock.getSoLinger() +
                            ", sndBuf=" + sock.getSendBufferSize() + ", sndBuf=" + sock.getSendBufferSize() + ']');

                        sock.setKeepAlive(true);

                        sock.setSoTimeout(2000);

                        sock.setSendBufferSize(256 * 1024);

                        X.println("Socket [timeout=" + sock.getSoTimeout() + ", linger=" + sock.getSoLinger() +
                            ", sndBuf=" + sock.getSendBufferSize() + ", rcvBuf=" + sock.getReceiveBufferSize() + ']');

                        while (!done.get())
                            X.println("Read from socket: " + sock.getInputStream().read());

                        return null;
                    }
                    finally {
                        U.closeQuiet(srvSock);
                        U.closeQuiet(sock);
                    }
                }
            },
            1,
            "server"
        );

        IgniteInternalFuture<?> fut2 = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    Socket sock = null;

                    try {
                        sock = new Socket(addr, 60000);

                        X.println("Socket [timeout=" + sock.getSoTimeout() + ", linger=" + sock.getSoLinger() +
                            ", sndBuf=" + sock.getSendBufferSize() + ", sndBuf=" + sock.getSendBufferSize() + ']');

                        sock.setKeepAlive(true);

                        sock.setSoTimeout(2000);

                        sock.setSendBufferSize(256 * 1024);

                        X.println("Socket [timeout=" + sock.getSoTimeout() + ", linger=" + sock.getSoLinger() +
                            ", sndBuf=" + sock.getSendBufferSize() + ", sndBuf=" + sock.getSendBufferSize() + ']');

                        int i = 0;

                        while (!done.get()) {
                            sock.getOutputStream().write(++i);

                            sock.getOutputStream().flush();

                            X.println("Wrote to socket: " + i);

                            X.println("Socket connected: " + sock.isConnected());
                            X.println("Socket keep alive: " + sock.getKeepAlive());

                            U.sleep(1000);
                        }

                        return null;
                    }
                    finally {
                        U.closeQuiet(sock);
                    }
                }
            },
            1,
            "client"
        );

        JOptionPane.showMessageDialog(null, "Unplug network cable." + U.nl() +
            "Press OK to finish.");

        done.set(true);

        fut1.get();
        fut2.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadTimeout() throws Exception {
        final InetAddress addr = InetAddress.getByName("192.168.3.10");

        IgniteInternalFuture<?> fut1 = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    ServerSocket srvSock = null;
                    Socket sock = null;

                    try {
                        srvSock = new ServerSocket(60000, 0, addr);

                        sock = srvSock.accept();

                        X.println("Socket [timeout=" + sock.getSoTimeout() + ", linger=" + sock.getSoLinger() +
                            ", sndBuf=" + sock.getSendBufferSize() + ", sndBuf=" + sock.getSendBufferSize() +
                            ", NODELAY=" + sock.getTcpNoDelay() + ']');

                        sock.setSoTimeout(2000);
                        sock.setTcpNoDelay(true);

                        X.println("Socket [timeout=" + sock.getSoTimeout() + ", linger=" + sock.getSoLinger() +
                            ", sndBuf=" + sock.getSendBufferSize() + ", sndBuf=" + sock.getSendBufferSize() +
                            ", NODELAY=" + sock.getTcpNoDelay() + ']');

                        sock.getInputStream().read();
                    }
                    catch (IOException e) {
                        X.println("Caught expected exception: " + e);

                        e.printStackTrace();
                    }
                    finally {
                        U.closeQuiet(srvSock);
                        U.closeQuiet(sock);
                    }

                    return null;
                }
            },
            1,
            "server"
        );

        IgniteInternalFuture<?> fut2 = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    Socket sock = null;

                    try {
                        sock = new Socket(addr, 60000);

                        X.println("Socket [timeout=" + sock.getSoTimeout() + ", linger=" + sock.getSoLinger() +
                            ", sndBuf=" + sock.getSendBufferSize() + ", sndBuf=" + sock.getSendBufferSize() +
                            ", NODELAY=" + sock.getTcpNoDelay() + ']');

                        sock.setTcpNoDelay(true);

                        X.println("Socket [timeout=" + sock.getSoTimeout() + ", linger=" + sock.getSoLinger() +
                            ", sndBuf=" + sock.getSendBufferSize() + ", sndBuf=" + sock.getSendBufferSize() +
                            ", NODELAY=" + sock.getTcpNoDelay() + ']');

                        Thread.sleep(10000);

                        return null;
                    }
                    finally {
                        U.closeQuiet(sock);
                    }
                }
            },
            1,
            "client"
        );

        fut1.get();
        fut2.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSocketCloseOnTimeout() throws Exception {
        final AtomicBoolean done = new AtomicBoolean();

        final InetAddress addr = InetAddress.getByName("192.168.0.100");

        IgniteInternalFuture<?> fut1 = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    ServerSocket srvSock = null;
                    Socket sock = null;

                    try {
                        srvSock = new ServerSocket(60000, 0, addr);

                        sock = srvSock.accept();

                        while (!done.get())
                            U.sleep(1000);

                        return null;
                    }
                    finally {
                        U.closeQuiet(srvSock);
                        U.closeQuiet(sock);
                    }
                }
            },
            1,
            "server"
        );

        final AtomicReference<Socket> sockRef = new AtomicReference<>();

        IgniteInternalFuture<?> fut2 = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    Socket sock = null;

                    try {
                        sock = new Socket(addr, 60000);

                        X.println("Socket [timeout=" + sock.getSoTimeout() + ", linger=" + sock.getSoLinger() +
                            ", sndBuf=" + sock.getSendBufferSize() + ", sndBuf=" + sock.getSendBufferSize() + ']');

                        sockRef.set(sock);

                        sock.getOutputStream().write(
                            new byte[(sock.getSendBufferSize() + sock.getReceiveBufferSize()) * 2]);

                        assert false : "Message has been written.";
                    }
                    catch (IOException e) {
                        X.println("Caught expected exception: " + e);

                        e.printStackTrace();
                    }
                    finally {
                        U.closeQuiet(sock);
                    }

                    return null;
                }
            },
            1,
            "client"
        );

        IgniteInternalFuture<?> fut3 = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    while (true) {
                        U.sleep(1000);

                        Socket sock = sockRef.get();

                        if (sock != null) {
                            U.sleep(1000);

                            U.closeQuiet(sock);

                            return null;
                        }
                    }
                }
            },
            1,
            "client"
        );

        fut2.get();
        fut3.get();

        done.set(true);

        fut1.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testConnectionTime() throws Exception {
        X.println("Unexistent host.");
        checkConnection(InetAddress.getByName("192.168.0.222"));

        X.println("Unlistened port.");
        checkConnection(InetAddress.getByName("192.168.0.1"));
    }

    /**
     * @param addr Address to check connection to.
     */
    private void checkConnection(InetAddress addr) {
        long start = System.currentTimeMillis();

        Socket sock = null;

        try {
            sock = openSocket(addr, 80);
        }
        catch (Exception e) {
            X.println("Caught exception: " + e.getClass().getSimpleName() + " - " + e.getMessage());
        }
        finally {
            X.println("Time taken: " + (System.currentTimeMillis() - start));

            U.closeQuiet(sock);
        }
    }

    /**
     * @param addr Remote address.
     * @param port Remote port.
     * @return Opened socket.
     * @throws IOException If failed.
     */
    private Socket openSocket(InetAddress addr, int port) throws IOException {
        Socket sock = new Socket();

        sock.bind(new InetSocketAddress(InetAddress.getByName("192.168.0.100"), 0));

        sock.connect(new InetSocketAddress(addr, port), 1);

        X.println("Socket [timeout=" + sock.getSoTimeout() + ", linger=" + sock.getSoLinger() +
            ", sndBuf=" + sock.getSendBufferSize() + ", sndBuf=" + sock.getSendBufferSize() + ']');

        return sock;
    }
}