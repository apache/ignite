/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Sergi Vladykin
 */
package org.h2.test.unit;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import org.h2.engine.SysProperties;
import org.h2.test.TestBase;
import org.h2.util.NetUtils;
import org.h2.util.Task;

/**
 * Test the network utilities from {@link NetUtils}.
 *
 * @author Sergi Vladykin
 * @author Tomas Pospichal
 */
public class TestNetUtils extends TestBase {

    private static final int WORKER_COUNT = 10;
    private static final int PORT = 9111;
    private static final int WAIT_MILLIS = 100;
    private static final int WAIT_LONGER_MILLIS = 2 * WAIT_MILLIS;
    private static final String TASK_PREFIX = "ServerSocketThread-";

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        testAnonymousTlsSession();
        testTlsSessionWithServerSideAnonymousDisabled();
        testFrequentConnections(true, 100);
        testFrequentConnections(false, 1000);
    }

    /**
     * With default settings, H2 client SSL socket should be able to connect
     * to an H2 server SSL socket using an anonymous cipher suite
     * (no SSL certificate is needed).
     */
    private void testAnonymousTlsSession() throws Exception {
        assertTrue("Failed assumption: the default value of ENABLE_ANONYMOUS_TLS" +
                " property should be true", SysProperties.ENABLE_ANONYMOUS_TLS);
        boolean ssl = true;
        Task task = null;
        ServerSocket serverSocket = null;
        Socket socket = null;

        try {
            serverSocket = NetUtils.createServerSocket(PORT, ssl);
            serverSocket.setSoTimeout(WAIT_LONGER_MILLIS);
            task = createServerSocketTask(serverSocket);
            task.execute(TASK_PREFIX + "AnonEnabled");
            Thread.sleep(WAIT_MILLIS);
            socket = NetUtils.createLoopbackSocket(PORT, ssl);
            assertTrue("loopback anon socket should be connected", socket.isConnected());
            SSLSession session = ((SSLSocket) socket).getSession();
            assertTrue("TLS session should be valid when anonymous TLS is enabled",
                    session.isValid());
            // in case of handshake failure:
            // the cipher suite is the pre-handshake SSL_NULL_WITH_NULL_NULL
            assertContains(session.getCipherSuite(), "_anon_");
        } finally {
            closeSilently(socket);
            closeSilently(serverSocket);
            if (task != null) {
                // SSL server socket should succeed using an anonymous cipher
                // suite, and not throw javax.net.ssl.SSLHandshakeException
                assertNull(task.getException());
                task.join();
            }
        }
    }

    /**
     * TLS connections (without trusted certificates) should fail if the server
     * does not allow anonymous TLS.
     * The global property ENABLE_ANONYMOUS_TLS cannot be modified for the test;
     * instead, the server socket is altered.
     */
    private void testTlsSessionWithServerSideAnonymousDisabled() throws Exception {
        boolean ssl = true;
        Task task = null;
        ServerSocket serverSocket = null;
        Socket socket = null;
        try {
            serverSocket = NetUtils.createServerSocket(PORT, ssl);
            serverSocket.setSoTimeout(WAIT_LONGER_MILLIS);
            // emulate the situation ENABLE_ANONYMOUS_TLS=false on server side
            String[] defaultCipherSuites = SSLContext.getDefault().getServerSocketFactory()
                    .getDefaultCipherSuites();
            ((SSLServerSocket) serverSocket).setEnabledCipherSuites(defaultCipherSuites);
            task = createServerSocketTask(serverSocket);
            task.execute(TASK_PREFIX + "AnonDisabled");
            Thread.sleep(WAIT_MILLIS);
            socket = NetUtils.createLoopbackSocket(PORT, ssl);
            assertTrue("loopback socket should be connected", socket.isConnected());
            // Java 6 API does not have getHandshakeSession() which could
            // reveal the actual cipher selected in the attempted handshake
            SSLSession session = ((SSLSocket) socket).getSession();
            assertFalse("TLS session should be invalid when the server" +
                    "disables anonymous TLS", session.isValid());
            // the SSL handshake should fail, because non-anon ciphers require
            // a trusted certificate
            assertEquals("SSL_NULL_WITH_NULL_NULL", session.getCipherSuite());
        } finally {
            closeSilently(socket);
            closeSilently(serverSocket);
            if (task != null) {
                assertTrue(task.getException() != null);
                assertEquals(javax.net.ssl.SSLHandshakeException.class.getName(),
                        task.getException().getClass().getName());
                assertContains(task.getException().getMessage(), "certificate_unknown");
                task.join();
            }
        }
    }

    private Task createServerSocketTask(final ServerSocket serverSocket) {
        Task task = new Task() {

            @Override
            public void call() throws Exception {
                Socket ss = null;
                try {
                    ss = serverSocket.accept();
                    ss.getOutputStream().write(123);
                } finally {
                    closeSilently(ss);
                }
            }
        };
        return task;
    }

    /**
     * Close a socket, ignoring errors
     *
     * @param socket the socket
     */
    void closeSilently(Socket socket) {
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (Exception e) {
            // ignore
        }
    }

    /**
     * Close a server socket, ignoring errors
     *
     * @param socket the server socket
     */
    void closeSilently(ServerSocket socket) {
        try {
            socket.close();
        } catch (Exception e) {
            // ignore
        }
    }

    private static void testFrequentConnections(boolean ssl, int count) throws Exception {
        final ServerSocket serverSocket = NetUtils.createServerSocket(PORT, ssl);
        final AtomicInteger counter = new AtomicInteger(count);
        Task serverThread = new Task() {
            @Override
            public void call() {
                while (!stop) {
                    try {
                        Socket socket = serverSocket.accept();
                        // System.out.println("opened " + counter);
                        socket.close();
                    } catch (Exception e) {
                        // ignore
                    }
                }
                // System.out.println("stopped ");

            }
        };
        serverThread.execute();
        try {
            Set<ConnectWorker> workers = new HashSet<>();
            for (int i = 0; i < WORKER_COUNT; i++) {
                workers.add(new ConnectWorker(ssl, counter));
            }
            // ensure the server is started
            Thread.sleep(100);
            for (ConnectWorker worker : workers) {
                worker.start();
            }
            for (ConnectWorker worker : workers) {
                worker.join();
                Exception e = worker.getException();
                if (e != null) {
                    e.printStackTrace();
                }
            }
        } finally {
            try {
                serverSocket.close();
            } catch (Exception e) {
                // ignore
            }
            serverThread.get();
        }
    }

    /**
     * A worker thread to test connecting.
     */
    private static class ConnectWorker extends Thread {

        private final boolean ssl;
        private final AtomicInteger counter;
        private Exception exception;

        ConnectWorker(boolean ssl, AtomicInteger counter) {
            this.ssl = ssl;
            this.counter = counter;
        }

        @Override
        public void run() {
            try {
                while (!isInterrupted() && counter.decrementAndGet() > 0) {
                    Socket socket = NetUtils.createLoopbackSocket(PORT, ssl);
                    try {
                        socket.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            } catch (Exception e) {
                exception = new Exception("count: " + counter, e);
            }
        }

        public Exception getException() {
            return exception;
        }

    }

}
