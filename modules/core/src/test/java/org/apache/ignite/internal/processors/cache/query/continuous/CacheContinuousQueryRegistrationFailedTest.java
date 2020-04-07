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

import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/** */
public class CacheContinuousQueryRegistrationFailedTest extends GridCommonAbstractTest {
    /** */
    private static final String EXT_EVT_FILTER_CLS = "org.apache.ignite.tests.p2p.CacheDeploymentEntryEventFilter";

    /** */
    private final ClassLoader extLdr = getExternalClassLoader();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (getTestIgniteInstanceName(0).equals(igniteInstanceName))
            cfg.setClassLoader(extLdr);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(true);
    }

    /** */
    @Test
    public void testRegistrationFailureOnServer() throws Exception {
        checkRegistraionFailure(false);
    }

    /** */
    @Test
    public void testRegistrationFailureOnClient() throws Exception {
        checkRegistraionFailure(true);
    }

    /** */
    public void checkRegistraionFailure(boolean isClient) throws Exception {
        IgniteEx ignite = startGrid(0);

        if (isClient)
            startClientGrid(1);
        else
            startGrid(1);

        ignite.cluster().state(ACTIVE);

        ignite.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        Class<CacheEntryEventFilter<Integer, Integer>> filterCls =
            (Class<CacheEntryEventFilter<Integer, Integer>>)extLdr.loadClass(EXT_EVT_FILTER_CLS);

        Factory<CacheEntryEventFilter<Integer, Integer>> rmtFilterFactory = new ClassFilterFactory(filterCls);

        qry.setRemoteFilterFactory(rmtFilterFactory);

        assertThrowsAnyCause(
            log,
            () -> ignite.cache(DEFAULT_CACHE_NAME).query(qry),
            CacheException.class,
            "Failed to start continuous query."
        );

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
