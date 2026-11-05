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

import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteException;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.sslTrustedFactory;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/** */
public class SslContextFactoryInitTest {
    /** */
    @Test
    public void testInitReplacesContextAfterLoad() throws Exception {
        SslContextFactory factory = spy((SslContextFactory)sslTrustedFactory("node01", "trustone"));

        factory.setProtocols("TLSv1.2");

        SSLContext original = factory.create();
        AtomicReference<SSLContext> duringInit = new AtomicReference<>();
        String keyStorePath = factory.getKeyStoreFilePath();

        // Pause init() just before it opens the key store and read the cached context from another thread.
        doAnswer(inv -> {
            duringInit.set(runAsync(factory::create).get(10_000));

            return inv.callRealMethod();
        }).doCallRealMethod().when(factory).openFileInputStream(keyStorePath);

        factory.setProtocols("TLSv1.3");
        factory.init();

        assertSame(original, duringInit.get());
        assertArrayEquals(new String[] {"TLSv1.3"}, factory.create().createSSLEngine().getEnabledProtocols());
    }

    /** */
    @Test
    public void testCreateUsesContextSetAfterInit() {
        SslContextFactory factory = (SslContextFactory)sslTrustedFactory("node01", "trustone");
        SSLContext original = factory.create();

        factory.init();

        factory.sslCtx.set(original);

        assertSame(original, factory.create());
    }

    /** */
    @Test
    public void testFailedInitKeepsContext() {
        SslContextFactory factory = (SslContextFactory)sslTrustedFactory("node01", "trustone");
        SSLContext original = factory.create();

        factory.setKeyStorePassword("wrong-password".toCharArray());

        assertThrows(IgniteException.class, factory::init);
        assertSame(original, factory.create());
    }
}
