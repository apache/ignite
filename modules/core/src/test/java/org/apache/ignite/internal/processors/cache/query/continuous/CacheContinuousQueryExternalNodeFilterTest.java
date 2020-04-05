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

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.CQ_SYS_VIEW;
import static org.apache.ignite.testframework.config.GridTestProperties.getProperty;

/** */
public class CacheContinuousQueryExternalNodeFilterTest extends GridCommonAbstractTest {
    /** */
    private static final String EXT_EVT_FILTER_CLS = "org.apache.ignite.tests.p2p.CacheDeploymentEntryEventFilter";

    /** */
    private static final String EXT_NODE_FILTER_CLS = "org.apache.ignite.tests.p2p.AttributeBasedNodeFilter";

    /** */
    private static final URL[] URLS;

    static {
        try {
            URLS = new URL[] {new URL(getProperty("p2p.uri.cls.second"))};
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    private final ClassLoader extLdr = getExternalClassLoader();

    /** */
    private final ClassLoader secondExtLdr = new URLClassLoader(URLS, U.gridClassLoader());

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (getTestIgniteInstanceName(0).equals(igniteInstanceName))
            cfg.setClassLoader(extLdr);
        else {
            cfg.setClassLoader(secondExtLdr);
            cfg.setUserAttributes(U.map("skipCacheStart", true));
        }

        return cfg;
    }

    /** */
    @Test
    public void test() throws Exception {
        startGrids(2).cluster().state(ClusterState.ACTIVE);

        Class<IgnitePredicate<ClusterNode>> nodeFilter = (Class<IgnitePredicate<ClusterNode>>)extLdr
            .loadClass(EXT_NODE_FILTER_CLS);

        grid(0).createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setNodeFilter(nodeFilter.newInstance()));

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        Class<CacheEntryEventFilter<Integer, Integer>> filterCls =
            (Class<CacheEntryEventFilter<Integer, Integer>>)extLdr.loadClass(EXT_EVT_FILTER_CLS);

        Factory<CacheEntryEventFilter<Integer, Integer>> rmtFilterFactory = new ClassFilterFactory(filterCls);

        qry.setRemoteFilterFactory(rmtFilterFactory);

        assertNull(grid(1).context().cache().internalCache(DEFAULT_CACHE_NAME));

        grid(0).cache(DEFAULT_CACHE_NAME).query(qry);

        assertEquals(1, grid(0).context().systemView().view(CQ_SYS_VIEW).size());
        assertEquals(0, grid(1).context().systemView().view(CQ_SYS_VIEW).size());
    }

    /** */
    private static class ClassFilterFactory implements Factory<CacheEntryEventFilter<Integer, Integer>> {
        /** */
        private Class<CacheEntryEventFilter<Integer, Integer>> cls;

        /** */
        public ClassFilterFactory(Class<CacheEntryEventFilter<Integer, Integer>> cls) {
            this.cls = cls;
        }

        /** {@inheritDoc} */
        @Override public CacheEntryEventFilter<Integer, Integer> create() {
            try {
                return cls.newInstance();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
