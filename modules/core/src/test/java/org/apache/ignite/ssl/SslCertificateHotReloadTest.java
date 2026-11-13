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

import java.nio.file.Files;
import java.nio.file.Path;
import javax.net.ssl.SSLContext;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.ignite.ssl.TestCertificateAuthority.PASSWORD;
import static org.apache.ignite.ssl.TestCertificateAuthority.save;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class SslCertificateHotReloadTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testNodeHotReloadsCertificatesForNewConnections() throws Exception {
        long now = System.currentTimeMillis();
        TestCertificateAuthority ca1 = new TestCertificateAuthority("ca1", now - DAYS.toMillis(1), now + DAYS.toMillis(10));
        TestCertificateAuthority ca2 = new TestCertificateAuthority("ca2", now - DAYS.toMillis(1), now + DAYS.toMillis(10));
        Path dir = Files.createTempDirectory("ssl-hot-reload");
        Path keys = dir.resolve("keys.jks");
        Path trust = dir.resolve("trust.jks");

        try {
            save(ca1.keyStore("node", now - MINUTES.toMillis(110), now + MINUTES.toMillis(10)), keys);
            save(ca1.trustStore(), trust);

            SslContextFactory serverFactory = configure(new SslContextFactory(), keys, trust);

            serverFactory.setHotReloadEnabled(true);

            SSLContext initCtx = serverFactory.create();

            assertTrue("Hot reload must be due at startup", serverFactory.getHotReloadTime() < now);

            save(ca2.keyStore("node", now - MINUTES.toMillis(1), now + DAYS.toMillis(1)), keys);
            save(ca2.trustStore(), trust);

            IgniteEx node = startGrid(0, cfg -> {
                cfg.setClientConnectorConfiguration(sslClientConnector(serverFactory));
            });

            assertTrue("The reloaded context was not installed", waitForCondition(() -> serverFactory.create() != initCtx, 10_000));

            ClientConfiguration clientCfg = new ClientConfiguration()
                .setAddresses("127.0.0.1:" + node.context().clientListener().port())
                .setSslMode(SslMode.REQUIRED)
                .setSslContextFactory(configure(new SslContextFactory(), keys, trust));

            try (IgniteClient client = Ignition.startClient(clientCfg)) {
                client.cacheNames();
            }
        }
        finally {
            stopAllGrids();
            U.delete(dir);
        }
    }

    /** */
    @Test
    public void testHotReloadRetriesWhileSourceHoldsExpiredCertificates() throws Exception {
        long now = System.currentTimeMillis();
        TestCertificateAuthority ca = new TestCertificateAuthority("ca", now - DAYS.toMillis(1), now + DAYS.toMillis(10));
        Path dir = Files.createTempDirectory("ssl-hot-reload");
        Path keys = dir.resolve("keys.jks");
        Path trust = dir.resolve("trust.jks");
        ListeningTestLogger nodeLog = new ListeningTestLogger(log);
        LogListener failure = LogListener.matches("TLS hot reload failed [transports=client connector").build();
        LogListener reason = LogListener.matches("Loaded certificates are still due for hot reload").build();

        nodeLog.registerListener(failure);
        nodeLog.registerListener(reason);

        try {
            save(ca.keyStore("node", now - MINUTES.toMillis(120), now - MINUTES.toMillis(1)), keys);
            save(ca.trustStore(), trust);

            SslContextFactory serverFactory = configure(new SslContextFactory(), keys, trust);

            serverFactory.setHotReloadEnabled(true);

            IgniteEx node = startGrid(0, cfg -> {
                cfg.setGridLogger(nodeLog).setClientConnectorConfiguration(sslClientConnector(serverFactory));
            });
            LongMetric failures = node.context().metric().registry("ssl").findMetric("ConsecutiveHotReloadFailures");

            assertTrue("The automatic attempt did not fail", waitForCondition(() -> failures.value() == 1, 10_000));
            assertTrue(failure.check());
            assertTrue(reason.check());
        }
        finally {
            stopAllGrids();
            U.delete(dir);
        }
    }

    /** */
    private static ClientConnectorConfiguration sslClientConnector(SslContextFactory factory) {
        return new ClientConnectorConfiguration()
            .setSslEnabled(true)
            .setSslClientAuth(true)
            .setUseIgniteSslContextFactory(false)
            .setSslContextFactory(factory);
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
