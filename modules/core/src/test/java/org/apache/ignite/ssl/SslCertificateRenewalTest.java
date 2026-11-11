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

package org.apache.ignite.ssl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLContext;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientRetryNonePolicy;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.ssl.TestCertificateAuthority.PASSWORD;
import static org.apache.ignite.ssl.TestCertificateAuthority.save;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class SslCertificateRenewalTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testRenewalUpdatesNewConnectionsAndPreservesEstablishedConnection() throws Exception {
        long now = System.currentTimeMillis();
        TestCertificateAuthority ca1 = new TestCertificateAuthority("ca1", now - DAYS.toMillis(1), now + DAYS.toMillis(10));
        TestCertificateAuthority ca2 = new TestCertificateAuthority("ca2", now - DAYS.toMillis(1), now + DAYS.toMillis(10));
        Path dir = Files.createTempDirectory("ssl-renewal");
        Path keys = dir.resolve("keys.jks");
        Path trust = dir.resolve("trust.jks");
        AtomicBoolean blockRenewal = new AtomicBoolean();
        CountDownLatch renewalStarted = new CountDownLatch(1);
        CountDownLatch allowRenewal = new CountDownLatch(1);

        SslContextFactory serverFactory = new SslContextFactory() {
            /** */
            private static final long serialVersionUID = 0L;

            /** {@inheritDoc} */
            @Override protected InputStream openFileInputStream(String path) throws IOException {
                if (keys.toString().equals(path) && blockRenewal.get()) {
                    renewalStarted.countDown();

                    try {
                        if (!allowRenewal.await(30, SECONDS))
                            throw new IOException("Timed out waiting for the replacement key store");
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();

                        throw new IOException(e);
                    }
                }

                return super.openFileInputStream(path);
            }
        };

        try {
            save(ca1.keyStore("node", now - MINUTES.toMillis(110), now + MINUTES.toMillis(10)), keys);
            save(ca1.trustStore(), trust);
            configure(serverFactory, keys, trust);
            serverFactory.setRenewalEnabled(true);

            SSLContext initCtx = serverFactory.create();

            blockRenewal.set(true);

            IgniteEx node = startGrid(0, cfg -> {
                cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration()
                    .setSslEnabled(true)
                    .setSslClientAuth(true)
                    .setUseIgniteSslContextFactory(false)
                    .setSslContextFactory(serverFactory));
            });

            assertTrue("Automatic renewal did not start", renewalStarted.await(10, SECONDS));

            ClientConfiguration clientCfg = clientConfiguration(node, configure(new SslContextFactory(), keys, trust));

            try (IgniteClient connected = Ignition.startClient(clientCfg)) {
                connected.cacheNames();

                save(ca2.keyStore("node", now - MINUTES.toMillis(1), now + DAYS.toMillis(1)), keys);
                save(ca2.trustStore(), trust);
                allowRenewal.countDown();

                assertTrue("The renewed context was not installed", waitForCondition(() -> serverFactory.create() != initCtx, 10_000));

                try (IgniteClient newConnection = Ignition.startClient(clientConfiguration(node,
                    configure(new SslContextFactory(), keys, trust)))) {
                    newConnection.cacheNames();
                }

                connected.cacheNames();
            }
        }
        finally {
            allowRenewal.countDown();
            stopAllGrids();
            U.delete(dir);
        }
    }

    /** */
    private static ClientConfiguration clientConfiguration(IgniteEx node, SslContextFactory factory) {
        return new ClientConfiguration()
            .setAddresses("127.0.0.1:" + node.context().clientListener().port())
            .setSslMode(SslMode.REQUIRED)
            .setSslContextFactory(factory)
            .setRetryPolicy(new ClientRetryNonePolicy())
            .setPartitionAwarenessEnabled(false)
            .setClusterDiscoveryEnabled(false);
    }

    /** */
    private static SslContextFactory configure(SslContextFactory factory, Path keys, Path trust) {
        factory.setKeyStoreFilePath(keys.toString());
        factory.setKeyStorePassword(PASSWORD);
        factory.setTrustStoreFilePath(trust.toString());
        factory.setTrustStorePassword(PASSWORD);

        return factory;
    }
}
