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
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
@SuppressWarnings("unused")
public class GridCacheContinuousQueryNodesFilteringSelfTest extends GridCommonAbstractTest implements Serializable {
    /** */
    private static final String ENTRY_FILTER_CLS_NAME = "org.apache.ignite.tests.p2p.CacheDeploymentEntryEventFilter";

    /**
     * Test that node not matched by filter really does not try to participate in the query.
     * @throws Exception if failed.
     */
    public void testNodeWithoutAttributeExclusion() throws Exception {
        try (Ignite node1 = startNodeWithCache()) {
            //noinspection EmptyTryBlock
            try (Ignite node2 = startGrid("node2", getConfiguration("node2", false, null))) {
                // No-op.
            }
        }
    }

    /**
     * Test that node matched by filter and having filter instantiation problems fails for sure.
     * @throws Exception if failed.
     */
    public void testNodeWithAttributeFailure() throws Exception {
        try (Ignite node1 = startNodeWithCache()) {
            GridStringLogger log = new GridStringLogger();
            //noinspection EmptyTryBlock
            try (Ignite node2 = startGrid("node2", getConfiguration("node2", true, log))) {
                // No-op.
            }
            catch (IgniteException e) { //This is what we expect...
                assertTrue(log.toString().contains("TcpDiscoverSpi's message worker thread failed abnormally. " +
                    "Stopping the node in order to prevent cluster wide instability." + U.nl() +
                    "class org.apache.ignite.IgniteException: Class not found for continuous query remote filter " +
                    "[name=org.apache.ignite.tests.p2p.CacheDeploymentEntryEventFilter]"));
            }
            catch (Exception e) { // And nothing else.
                throw new AssertionError("Unexpected exception", e);
            }
        }
    }

    /**
     * Start first, attribute-bearing, node, create new cache and launch continuous query on it.
     * @return node.
     * @throws Exception if failed.
     */
    private Ignite startNodeWithCache() throws Exception {
        Ignite node1 = startGrid("node1", getConfiguration("node1", true, null));
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();
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
            }
        });
        RemoteFilterFactory.clsLdr = getExternalClassLoader();
        cache.query(qry);
        // Switch class loader before starting the second node.
        RemoteFilterFactory.clsLdr = getClass().getClassLoader();
        return node1;
    }

    /**
     * @param name grid name.
     * @param setAttr flag indicating whether class loader and node user attribute should be tuned.
     * @return Grid configuration w/specified name and, optionally, external class loader.
     * @throws Exception If failed.
     */
    private IgniteConfiguration getConfiguration(String name, boolean setAttr, GridStringLogger log) throws Exception {
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
        @Override public CacheEntryEventFilter<Integer, Integer> create() {
            try {
                Class<?> filterCls = clsLdr.loadClass(ENTRY_FILTER_CLS_NAME);
                assert CacheEntryEventFilter.class.isAssignableFrom(filterCls);
                //noinspection unchecked
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
