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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestThread;
import org.junit.Test;

import static java.lang.Thread.State.BLOCKED;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePassword;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePath;
import static org.apache.ignite.testframework.GridTestUtils.sslTrustedFactory;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** */
public class SslContextFactoryInitTest {
    /** */
    @Test
    public void testInitUsesUpdatedProtocols() {
        SslContextFactory factory = (SslContextFactory)sslTrustedFactory("node01", "trustone");

        factory.setProtocols("TLSv1.2");
        factory.create();

        factory.setProtocols("TLSv1.3");
        factory.init();

        assertArrayEquals(new String[] {"TLSv1.3"}, factory.create().createSSLEngine().getEnabledProtocols());
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

    /** */
    @Test
    public void testConcurrentCreateLoadsKeyStoreOnce() throws Exception {
        BlockingSslContextFactory factory = new BlockingSslContextFactory();
        GridTestThread first = new GridTestThread(factory::create);
        GridTestThread second = new GridTestThread(factory::create);

        first.start();

        try {
            assertTrue(factory.firstLoadStarted.await(10, SECONDS));

            second.start();

            assertTrue(waitForCondition(() -> second.getState() == BLOCKED || factory.loads.get() > 1, 10_000));
            assertEquals(1, factory.loads.get());
        }
        finally {
            factory.allowFirstLoad.countDown();
            first.join(10_000);
            second.join(10_000);
        }

        assertFalse(first.isAlive());
        assertFalse(second.isAlive());
        first.checkError();
        second.checkError();
        assertEquals(1, factory.loads.get());
    }

    /** */
    @Test
    public void testInitWaitsForInitialContextLoad() throws Exception {
        BlockingSslContextFactory factory = new BlockingSslContextFactory();
        GridTestThread first = new GridTestThread(factory::create);
        GridTestThread second = new GridTestThread(factory::init);

        first.start();

        try {
            assertTrue(factory.firstLoadStarted.await(10, SECONDS));

            second.start();

            assertTrue(waitForCondition(() -> second.getState() == BLOCKED || factory.loads.get() > 1, 10_000));
            assertEquals(1, factory.loads.get());
        }
        finally {
            factory.allowFirstLoad.countDown();
            first.join(10_000);
            second.join(10_000);
        }

        assertFalse(first.isAlive());
        assertFalse(second.isAlive());
        first.checkError();
        second.checkError();
        assertEquals(2, factory.loads.get());
    }

    /** */
    @Test
    public void testConcurrentInitLoadsOneKeyStoreAtATime() throws Exception {
        BlockingSslContextFactory factory = new BlockingSslContextFactory();
        GridTestThread first = new GridTestThread(factory::init);
        GridTestThread second = new GridTestThread(factory::init);

        first.start();

        try {
            assertTrue(factory.firstLoadStarted.await(10, SECONDS));

            second.start();

            assertTrue(waitForCondition(() -> second.getState() == BLOCKED || factory.loads.get() > 1, 10_000));
            assertEquals(1, factory.loads.get());
        }
        finally {
            factory.allowFirstLoad.countDown();
            first.join(10_000);
            second.join(10_000);
        }

        assertFalse(first.isAlive());
        assertFalse(second.isAlive());
        first.checkError();
        second.checkError();
        assertEquals(2, factory.loads.get());
    }

    /** */
    private static class BlockingSslContextFactory extends SslContextFactory {
        /** */
        private static final long serialVersionUID = 0;

        /** */
        private final CountDownLatch firstLoadStarted = new CountDownLatch(1);

        /** */
        private final CountDownLatch allowFirstLoad = new CountDownLatch(1);

        /** */
        private final AtomicInteger loads = new AtomicInteger();

        /** */
        private BlockingSslContextFactory() {
            setKeyStoreFilePath(keyStorePath("node01"));
            setKeyStorePassword(keyStorePassword().toCharArray());
            setTrustManagers(getDisabledTrustManager());
        }

        /** {@inheritDoc} */
        @Override protected InputStream openFileInputStream(String path) throws IOException {
            if (loads.incrementAndGet() == 1) {
                firstLoadStarted.countDown();

                U.awaitQuiet(allowFirstLoad);
            }

            return super.openFileInputStream(path);
        }
    }
}
