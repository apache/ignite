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
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** */
@SuppressWarnings("unused")
@RunWith(JUnit4.class)
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
        try (Ignite node1 = startNodeWithCache()) {
            GridStringLogger log = new GridStringLogger();

            try (Ignite node2 = startGrid("node2", getConfiguration("node2", true, log))) {
                fail();
            }
            catch (IgniteException ignored) {
                assertTrue(log.toString().contains("Class not found for continuous query remote filter " +
                    "[name=org.apache.ignite.tests.p2p.CacheDeploymentEntryEventFilter]"));
            }
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
