/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
@RunWith(JUnit4.class)
public class IgniteMarshallerCacheConcurrentReadWriteTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(REPLICATED);
        ccfg.setRebalanceMode(SYNC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentReadWrite() throws Exception {
        Ignite ignite = startGrid(0);

        Map<Integer, Object> data = new HashMap<>();

        final Map<Integer, byte[]> dataBytes = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            Object obj = null;

            switch (i % 10) {
                case 0: obj = new TestClass1(); break;
                case 1: obj = new TestClass2(); break;
                case 2: obj = new TestClass3(); break;
                case 3: obj = new TestClass4(); break;
                case 4: obj = new TestClass5(); break;
                case 5: obj = new TestClass6(); break;
                case 6: obj = new TestClass7(); break;
                case 7: obj = new TestClass8(); break;
                case 8: obj = new TestClass9(); break;
                case 9: obj = new TestClass10(); break;
                default: fail();
            }

            data.put(i, obj);

            dataBytes.put(i, ignite.configuration().getMarshaller().marshal(obj));
        }

        ignite.cache(DEFAULT_CACHE_NAME).putAll(data);

        stopGrid(0);

        for (int i = 0; i < 3; i++) {
            log.info("Iteration: " + i);

            final AtomicInteger idx = new AtomicInteger();

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int node = idx.getAndIncrement();

                    Ignite ignite = startGrid(node);

                    IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

                    for (Map.Entry<Integer, byte[]> e : dataBytes.entrySet()) {
                        Object obj = ignite.configuration().getMarshaller().unmarshal(e.getValue(), null);

                        cache.put(e.getKey(), obj);
                    }

                    ignite.cache(DEFAULT_CACHE_NAME).getAll(dataBytes.keySet());

                    return null;
                }
            }, 10, "test-thread");

            stopAllGrids();
        }
    }

    /**
     *
     */
    static class TestClass1 implements Serializable { }

    /**
     *
     */
    static class TestClass2 implements Serializable { }

    /**
     *
     */
    static class TestClass3 implements Serializable { }

    /**
     *
     */
    static class TestClass4 implements Serializable { }

    /**
     *
     */
    static class TestClass5 implements Serializable { }

    /**
     *
     */
    static class TestClass6 implements Serializable { }

    /**
     *
     */
    static class TestClass7 implements Serializable { }

    /**
     *
     */
    static class TestClass8 implements Serializable { }

    /**
     *
     */
    static class TestClass9 implements Serializable { }

    /**
     *
     */
    static class TestClass10 implements Serializable { }
}
