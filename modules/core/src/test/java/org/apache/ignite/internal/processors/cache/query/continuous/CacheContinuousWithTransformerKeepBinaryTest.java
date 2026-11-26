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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryEvent;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Checks that {@link ContinuousQueryWithTransformer} on a keep-binary cache delivers the transformer result
 * to the listener as it was returned, for both local and remote events.
 */
public class CacheContinuousWithTransformerKeepBinaryTest extends GridCommonAbstractTest {

    /** */
    private static final int DFLT_SERVER_NODE_CNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).cache(DEFAULT_CACHE_NAME).removeAll();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(DFLT_SERVER_NODE_CNT);

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(true);
    }

    /** */
    @Test
    public void testNonBinaryResultArrivesAsIs() throws Exception {
        List<Object> received = Collections.synchronizedList(new ArrayList<>());

        ContinuousQueryWithTransformer<Integer, Object, PersonId> qry = new ContinuousQueryWithTransformer<>();

        qry.setRemoteTransformerFactory(FactoryBuilder.factoryOf(new ToPersonId()));

        qry.setLocalListener(evts -> {
            for (Object e : evts)
                received.add(e);
        });

        IgniteCache<Integer, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        // Primary on node 0, where the query runs: its event is transformed locally.
        int locKey = primaryKey(cache);

        // Primary on node 1: its event is transformed there and sent to node 0.
        int rmtKey = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));

        try (QueryCursor<?> ignored = cache.withKeepBinary().query(qry)) {
            cache.put(locKey, locKey);
            cache.put(rmtKey, rmtKey);

            assertTrue(GridTestUtils.waitForCondition(() -> received.size() == 2, 5_000));

            for (Object o : received)
                assertTrue("Unexpected class: " + o.getClass().getName(), o instanceof PersonId);
        }
    }

    /** */
    @Test
    public void testBinaryValueResultArrivesAsIs() throws Exception {
        List<Object> received = Collections.synchronizedList(new ArrayList<>());

        ContinuousQueryWithTransformer<Integer, Object, Object> qry = new ContinuousQueryWithTransformer<>();

        qry.setRemoteTransformerFactory(FactoryBuilder.factoryOf(new ToValue()));

        qry.setLocalListener(evts -> {
            for (Object e : evts)
                received.add(e);
        });

        IgniteCache<Integer, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        // Primary on node 0, where the query runs: its event is transformed locally.
        int locKey = primaryKey(cache);

        // Primary on node 1: its event is transformed there and sent to node 0.
        int rmtKey = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));

        try (QueryCursor<?> ignored = cache.withKeepBinary().query(qry)) {
            cache.put(locKey, new PersonId(locKey));
            cache.put(rmtKey, new PersonId(rmtKey));

            assertTrue(GridTestUtils.waitForCondition(() -> received.size() == 2, 5_000));

            for (Object o : received)
                assertTrue("Unexpected class: " + o.getClass().getName(), o instanceof BinaryObject);
        }
    }

    /** */
    @Test
    public void testBinaryKeyResultArrivesAsIs() throws Exception {
        List<Object> received = Collections.synchronizedList(new ArrayList<>());

        ContinuousQueryWithTransformer<Object, Integer, Object> qry = new ContinuousQueryWithTransformer<>();

        qry.setRemoteTransformerFactory(FactoryBuilder.factoryOf(new ToKey()));

        qry.setLocalListener(evts -> {
            for (Object e : evts)
                received.add(e);
        });

        IgniteCache<Object, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        // Primary on node 0, where the query runs: its event is transformed locally.
        PersonId locKey = new PersonId(primaryKey(cache));

        // Primary on node 1: its event is transformed there and sent to node 0.
        PersonId rmtKey = new PersonId(primaryKey(grid(1).cache(DEFAULT_CACHE_NAME)));

        // Make sure one event is transformed locally and the other remotely.
        // Check that @AffinityKeyMapped keeps each key on the node primaryKey() found it for.
        assertTrue(grid(0).affinity(DEFAULT_CACHE_NAME).isPrimary(grid(0).localNode(), locKey));
        assertTrue(grid(0).affinity(DEFAULT_CACHE_NAME).isPrimary(grid(1).localNode(), rmtKey));

        try (QueryCursor<?> ignored = cache.withKeepBinary().query(qry)) {
            cache.put(locKey, locKey.id);
            cache.put(rmtKey, rmtKey.id);

            assertTrue(GridTestUtils.waitForCondition(() -> received.size() == 2, 5_000));

            for (Object o : received)
                assertTrue("Unexpected class: " + o.getClass().getName(), o instanceof BinaryObject);
        }
    }

    /** */
    private static class PersonId {
        /** So the key has the same owner as its id. */
        @AffinityKeyMapped
        final int id;

        /** */
        PersonId(int id) {
            this.id = id;
        }
    }

    /** */
    private static class ToPersonId implements IgniteClosure<CacheEntryEvent<? extends Integer, ?>, PersonId> {
        /** {@inheritDoc} */
        @Override public PersonId apply(CacheEntryEvent<? extends Integer, ?> evt) {
            return new PersonId(evt.getKey());
        }
    }

    /** */
    private static class ToValue implements IgniteClosure<CacheEntryEvent<? extends Integer, ?>, Object> {
        /** {@inheritDoc} */
        @Override public Object apply(CacheEntryEvent<? extends Integer, ?> evt) {
            return evt.getValue();
        }
    }

    /** */
    private static class ToKey implements IgniteClosure<CacheEntryEvent<?, ? extends Integer>, Object> {
        /** {@inheritDoc} */
        @Override public Object apply(CacheEntryEvent<?, ? extends Integer> evt) {
            return evt.getKey();
        }
    }
}
