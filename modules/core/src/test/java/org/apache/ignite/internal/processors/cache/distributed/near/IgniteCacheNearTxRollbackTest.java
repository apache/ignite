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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.communication.tcp.*;

import javax.cache.*;
import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;

/**
 *
 */
public class IgniteCacheNearTxRollbackTest extends IgniteCacheAbstractTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return new NearCacheConfiguration();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllRollback() throws Exception {
        IgniteCache<Integer, Integer> cache = jcache(0);

        Map<Integer, Integer> map = new LinkedHashMap<>();

        map.put(nearKey(cache), 1);
        map.put(primaryKey(cache), 1);

        TestCommunicationSpi spi = (TestCommunicationSpi)ignite(0).configuration().getCommunicationSpi();

        spi.sndFail = true;

        try {
            try {
                cache.putAll(map);

                fail("Put should fail.");
            }
            catch (CacheException e) {
                log.info("Expected exception: " + e);

                assertFalse(X.hasCause(e, AssertionError.class));
            }

            for (int i = 0; i < gridCount(); i++) {
                for (Integer key : map.keySet())
                    assertNull(jcache(i).localPeek(key));
            }

            spi.sndFail = false;

            cache.putAll(map);

            for (Map.Entry<Integer, Integer> e : map.entrySet())
                assertEquals(e.getValue(), cache.get(e.getKey()));
        }
        finally {
            spi.sndFail = false;
        }
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private volatile boolean sndFail;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Object msg0 = ((GridIoMessage)msg).message();

                if (sndFail && msg0 instanceof GridNearTxPrepareRequest)
                    throw new IgniteSpiException("Test error");
            }

            super.sendMessage(node, msg);
        }
    }
}
