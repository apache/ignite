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

import java.io.Serializable;
import java.util.Collections;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
@SuppressWarnings("unused")
public class GridCacheContinuousQueryNodesFilteringTest extends GridCommonAbstractTest implements Serializable {
    /** */
    private static final String ENTRY_FILTER_CLS_NAME = "org.apache.ignite.tests.p2p.CacheDeploymentEntryEventFilter";

    /**
     * Tests that node not matched by filter really does not try to participate in the query.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testNodeWithoutAttributeExclusion() throws Exception {
        try (Ignite node1 = startNodeWithCache()) {
            try (Ignite node2 = startGrid("node2", getConfiguration("node2", false, null))) {
                assertEquals(2, node2.cluster().nodes().size());
            }
        }
    }

    /**
     * Test that node matched by filter and having filter instantiation problems fails for sure.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testNodeWithAttributeFailure() throws Exception {
        expectFailure(IgniteException.class, "Class not found for continuous query remote filter ");
        expectFailure(IgniteException.class, "GridWorker ");
        expectFailure(InterruptedException.class);

        try (Ignite node1 = startNodeWithCache()) {
            ListeningTestLogger log = new ListeningTestLogger();
            LogListener lsnr = LogListener.matches("Class not found for continuous query remote filter " +
                "[name=org.apache.ignite.tests.p2p.CacheDeploymentEntryEventFilter]").atLeast(1).build();
            log.registerListener(lsnr);

            try (Ignite node2 = startGrid("node2", getConfiguration("node2", true, log))) {
                fail();
            }
            catch (IgniteException ignored) {
                assertTrue(lsnr.check());
            }
        }
        finally {
            expectNothing();
        }
    }

    /**
     * Start first, attribute-bearing, node, create new cache and launch continuous query on it.
     *
     * @return Node.
     * @throws Exception if failed.
     */
    private Ignite startNodeWithCache() throws Exception {
        Ignite node1 = startGrid("node1", getConfiguration("node1", true, null));

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        ccfg.setName("attrsTestCache");
        ccfg.setNodeFilter(new IgnitePredicate<ClusterNode>() {
            /** {@inheritDoc} */
            @Override public boolean apply(ClusterNode node) {
                return "data".equals(node.attribute("node-type"));
            }
        });

        IgniteCache<Integer, Integer> cache = node1.createCache(ccfg);

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        qry.setRemoteFilterFactory(new RemoteFilterFactory());
        qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            /** {@inheritDoc} */
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                // No-op.
            }
        });

        RemoteFilterFactory.clsLdr = getExternalClassLoader();

        cache.query(qry);

        // Switch class loader before starting the second node.
        RemoteFilterFactory.clsLdr = getClass().getClassLoader();

        return node1;
    }

    /**
     * @param name Node name.
     * @param setAttr Flag indicating whether node user attribute should be set.
     * @param log Logger.
     * @return Node configuration w/specified name.
     * @throws Exception If failed.
     */
    private IgniteConfiguration getConfiguration(String name, boolean setAttr, IgniteLogger log) throws Exception {
        IgniteConfiguration cfg = optimize(getConfiguration(name));

        if (setAttr)
            cfg.setUserAttributes(Collections.singletonMap("node-type", "data"));

        cfg.setGridLogger(log);

        return cfg;
    }

    /**
     *
     */
    private static class RemoteFilterFactory implements Factory<CacheEntryEventFilter<Integer, Integer>> {
        /** */
        private static ClassLoader clsLdr;

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public CacheEntryEventFilter<Integer, Integer> create() {
            try {
                Class<?> filterCls = clsLdr.loadClass(ENTRY_FILTER_CLS_NAME);

                assert CacheEntryEventFilter.class.isAssignableFrom(filterCls);

                return ((Class<CacheEntryEventFilter>)filterCls).newInstance();
            }
            catch (ClassNotFoundException e) {
                throw new IgniteException("Class not found for continuous query remote filter [name=" +
                    e.getMessage() + "]");
            }
            catch (Exception e) { // We really don't expect anything else fancy here.
                throw new AssertionError("Unexpected exception", e);
            }
        }
    }
}
