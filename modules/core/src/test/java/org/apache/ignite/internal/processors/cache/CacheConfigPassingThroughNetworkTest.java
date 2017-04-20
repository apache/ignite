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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Collection;

/**
 */
public class CacheConfigPassingThroughNetworkTest extends GridCommonAbstractTest {
    /** Configure cache at start. */
    private boolean configureCacheAtStart = true;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (configureCacheAtStart)
            cfg.setCacheConfiguration(getCacheConfiguration());

        return cfg;
    }

    /**
     *
     */
    private CacheConfiguration getCacheConfiguration() {
        return defaultCacheConfiguration().
            setTopologyValidator(new TestTopologyValidator());
    }

    /**
     *
     */
    public void testStartCacheAtFirstNodeandPassToSecond() throws Exception {
        try {
            IgniteEx igniteEx0 = startGrid(0);

            checkCacheInitialCfg(igniteEx0);

            configureCacheAtStart = false;

            IgniteEx igniteEx1 = startGrid(1);

            checkCacheInitialCfg(igniteEx1);

        }
        finally {
            stopAllGrids();
        }

    }

    /**
     *
     */
    public void testStartCacheDynamicPassToSecond() throws Exception {
        configureCacheAtStart = false;

        try {
            IgniteEx igniteEx0 = startGrid(0);

            IgniteEx igniteEx1 = startGrid(1);

            igniteEx0.createCache(getCacheConfiguration());

            checkCacheInitialCfg(igniteEx0);

            checkCacheInitialCfg(igniteEx1);

        }
        finally {
            stopAllGrids();
        }

    }

    private void checkCacheInitialCfg(IgniteEx igniteEx) {
        CacheConfiguration ccfg = igniteEx.context().cache().cacheDescriptor(null).startCacheConfiguration();

        TestTopologyValidator topologyValidator = (TestTopologyValidator)ccfg.getTopologyValidator();

        assertNotNull(topologyValidator);

        assertNull(topologyValidator.ignite);
    }

    private static class TestTopologyValidator implements TopologyValidator {

        @IgniteInstanceResource
        private /*transient*/ Ignite ignite;

        @Override public boolean validate(Collection<ClusterNode> nodes) {

            ignite.log().info("!!! CacheConfigPassingThroughNetworkTest validate topology on node " + ignite.name());

            return true;
        }
    }
}
