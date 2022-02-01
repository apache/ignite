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

package org.apache.ignite.internal.client.thin;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.configuration.ClientConnectorConfiguration.DFLT_PORT;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Thin client timeouts tests.
 */
public class TimeoutTest extends AbstractThinClientTest {
    /**
     * Default timeout value.
     */
    private static final int TIMEOUT = 500;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setClientConnectorConfiguration(
            new ClientConnectorConfiguration().setHandshakeTimeout(TIMEOUT));
    }

    /** {@inheritDoc} */
    @Override protected ClientConfiguration getClientConfiguration() {
        return super.getClientConfiguration().setTimeout(TIMEOUT);
    }

    /**
     * Test that server closes thin client connection in case of handshake timeout.
     */
    @Test
    public void testServerClosesThinClientConnectionOnHandshakeTimeout() {
        try (Ignite ignite = startGrid(0)) {
            long ts0 = System.currentTimeMillis();

            Socket s = new Socket();

            s.connect(new InetSocketAddress(clientHost(ignite.cluster().localNode()),
                clientPort(ignite.cluster().localNode())), 0);

            s.setSoTimeout(TIMEOUT * 2);

            OutputStream os = s.getOutputStream();

            try (BinaryOutputStream bos = new BinaryHeapOutputStream(32)) {
                bos.writeInt(1000); // Size.

                os.write(bos.arrayCopy());
                os.flush();

                InputStream is = s.getInputStream();

                assertEquals(-1, is.read()); // Connection and stream closed by server after timeout.

                long ts1 = System.currentTimeMillis();

                assertTrue("Unexpected timeout [ts0=" + ts0 + ", ts1=" + ts1 + ']',
                    ts1 - ts0 >= TIMEOUT && ts1 - ts0 < TIMEOUT * 2);
            }
            finally {
                s.close();
            }
        }
        catch (Exception e) {
            fail("Exception while sending message: " + e.getMessage());
        }
    }

    /**
     * Test client timeout on handshake.
     */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testClientTimeoutOnHandshake() throws Exception {
        ServerSocket sock = new ServerSocket();

        sock.bind(new InetSocketAddress("127.0.0.1", DFLT_PORT));

        AtomicBoolean connectionAccepted = new AtomicBoolean();

        CountDownLatch latch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            try {
                Socket accepted = sock.accept();

                connectionAccepted.set(true);

                latch.await(TIMEOUT * 2, TimeUnit.MILLISECONDS);

                U.closeQuiet(accepted);
            }
            catch (Exception e) {
                throw new IgniteException("Accept thread failed: " + e.getMessage(), e);
            }
        });

        long ts0 = System.currentTimeMillis();

        try {
            GridTestUtils.assertThrowsWithCause(
                (Runnable)() -> Ignition.startClient(getClientConfiguration().setAddresses("127.0.0.1:" + DFLT_PORT)),
                ClientConnectionException.class);
        }
        finally {
            latch.countDown();
        }

        U.closeQuiet(sock);

        assertTrue(connectionAccepted.get());

        long ts1 = System.currentTimeMillis();

        assertTrue("Unexpected timeout [ts0=" + ts0 + ", ts1=" + ts1 + ']',
            ts1 - ts0 >= TIMEOUT && ts1 - ts0 < TIMEOUT * 2);

        fut.get();
    }

    /**
     * Test client timeout on operation.
     */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testClientTimeoutOnOperation() throws Exception {
        try (Ignite ignite = startGrid(0)) {
            try (IgniteClient client = startClient(0)) {
                ClientCache<Object, Object> cache = client.getOrCreateCache(new ClientCacheConfiguration()
                    .setName("cache").setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

                doSleep(TIMEOUT * 2);

                // Should not fail if connection is idle.
                cache.put(0, 0);

                CyclicBarrier barrier = new CyclicBarrier(2);

                IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
                    try (ClientTransaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        cache.put(0, 0);

                        barrier.await(TIMEOUT * 2, TimeUnit.MILLISECONDS);
                        barrier.await(TIMEOUT * 2, TimeUnit.MILLISECONDS);
                    }
                    catch (Exception e) {
                        throw new IgniteException(e);
                    }
                });

                // Wait for the key locked.
                barrier.await(TIMEOUT * 2, TimeUnit.MILLISECONDS);

                long ts0 = System.currentTimeMillis();

                try (ClientTransaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    try {
                        GridTestUtils.assertThrowsWithCause(() -> cache.put(0, 0), ClientException.class);
                    }
                    finally {
                        // To unlock another thread.
                        barrier.await(TIMEOUT * 2, TimeUnit.MILLISECONDS);
                    }
                }

                long ts1 = System.currentTimeMillis();

                assertTrue("Unexpected timeout [ts0=" + ts0 + ", ts1=" + ts1 + ']',
                    ts1 - ts0 >= TIMEOUT && ts1 - ts0 < TIMEOUT * 2);

                fut.get();
            }
        }
    }
}
