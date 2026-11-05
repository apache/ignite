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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.keyStorePath;
import static org.apache.ignite.testframework.GridTestUtils.sslTrustedFactory;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/** */
public class SslContextFactoryInitTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testInitReplacesContext() {
        SslContextFactory factory = factory();
        String[] oldProtocols = {"TLSv1.2"};
        String[] newProtocols = {"TLSv1.3"};

        factory.setProtocols(oldProtocols);

        SSLContext original = factory.create();

        factory.setProtocols(newProtocols);
        factory.init();

        SSLContext replacement = factory.create();

        assertNotSame(original, replacement);
        assertSame(replacement, factory.sslCtx.get());
        assertArrayEquals(oldProtocols, original.createSSLEngine().getEnabledProtocols());
        assertArrayEquals(newProtocols, replacement.createSSLEngine().getEnabledProtocols());

        factory.sslCtx.set(original);

        assertSame(original, factory.create());
    }

    /** */
    @Test
    public void testFailedInitKeepsContext() {
        SslContextFactory factory = factory();
        SSLContext original = factory.create();

        factory.setKeyStorePassword("wrong-password".toCharArray());

        assertThrows(IgniteException.class, factory::init);
        assertSame(original, factory.create());
    }

    /** */
    @Test
    public void testCreateReturnsCurrentContextDuringInit() throws Exception {
        SslContextFactory factory = spy(factory());
        SSLContext original = factory.create();
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        doAnswer(inv -> {
            loading.countDown();
            assertTrue(release.await(10, TimeUnit.SECONDS));

            return inv.callRealMethod();
        }).when(factory).loadKeyStore(anyString(), eq(keyStorePath("node01")), any(char[].class));

        IgniteInternalFuture<Void> reload = GridTestUtils.runAsync(() -> factory.init());

        try {
            assertTrue(loading.await(10, TimeUnit.SECONDS));
            assertSame(original, GridTestUtils.runAsync(() -> factory.create()).get(10_000));
        }
        finally {
            release.countDown();
            reload.get(10_000);
        }

        assertNotSame(original, factory.create());
    }

    /** */
    private static SslContextFactory factory() {
        return (SslContextFactory)sslTrustedFactory("node01", "trustone");
    }
}
