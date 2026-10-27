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

import java.security.KeyStore;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool;
import org.apache.ignite.testframework.TestCertificateAuthority;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** Tests that communication connections reuse the context loaded at startup. */
public class SslContextReuseTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** Opening and reopening a communication connection must not invoke the SSL factory again. */
    @Test
    public void testCommunicationReusesContextOnReconnect() throws Exception {
        TestCertificateAuthority ca = new TestCertificateAuthority("communication-ca");
        CountingFactory firstFactory = new CountingFactory(ca);
        CountingFactory secondFactory = new CountingFactory(ca);
        IgniteEx first = startGrid(0, cfg -> {
            cfg.setSslContextFactory(firstFactory);
        });
        IgniteEx second = startGrid(1, cfg -> {
            cfg.setSslContextFactory(secondFactory);
        });
        int firstStartupCalls = firstFactory.calls.get();
        int secondStartupCalls = secondFactory.calls.get();

        assertEquals((Integer)1, first.compute(first.cluster().forNodeId(second.localNode().id())).call(() -> 1));

        ConnectionClientPool pool = U.field(first.configuration().getCommunicationSpi(), "clientPool");

        pool.forceCloseConnection(second.localNode().id());

        assertEquals((Integer)1, first.compute(first.cluster().forNodeId(second.localNode().id())).call(() -> 1));
        assertEquals(firstStartupCalls, firstFactory.calls.get());
        assertEquals(secondStartupCalls, secondFactory.calls.get());
    }

    /** A factory that builds a fresh context on every call from mutually trusted test material. */
    private static class CountingFactory implements Factory<SSLContext> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final KeyStore keys;

        /** */
        private final KeyStore trust;

        /** */
        private final AtomicInteger calls = new AtomicInteger();

        /** @param ca Authority shared by both nodes. */
        CountingFactory(TestCertificateAuthority ca) throws Exception {
            keys = ca.keyStoreWithPrivateKey();
            trust = ca.trustStore();
        }

        /** {@inheritDoc} */
        @Override public SSLContext create() {
            calls.incrementAndGet();

            try {
                return TestCertificateAuthority.context(keys, trust);
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }
    }
}
