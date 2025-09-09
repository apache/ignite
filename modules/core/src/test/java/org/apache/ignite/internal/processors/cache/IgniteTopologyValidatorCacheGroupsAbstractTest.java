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

import java.io.Serializable;
import java.util.Collection;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.CacheTopologyValidatorProvider;
import org.junit.Test;

/**
 *
 */
public abstract class IgniteTopologyValidatorCacheGroupsAbstractTest extends IgniteTopologyValidatorAbstractCacheTest {
    /** group name 1. */
    protected static final String GROUP_1 = "group1";

    /** group name 2. */
    protected static final String GROUP_2 = "group2";

    /** cache name 3. */
    protected static String CACHE_NAME_3 = "cache3";

    /** cache name 4. */
    protected static String CACHE_NAME_4 = "cache4";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration icfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration[] ccfgs = F.concat(
            icfg.getCacheConfiguration(),
            cacheConfiguration(igniteInstanceName).setName(CACHE_NAME_3),
            cacheConfiguration(igniteInstanceName).setName(CACHE_NAME_4)
        );

        for (CacheConfiguration ccfg : ccfgs) {
            if (CACHE_NAME_1.equals(ccfg.getName()) || CACHE_NAME_2.equals(ccfg.getName()))
                ccfg.setGroupName(GROUP_1);
            else if (CACHE_NAME_3.equals(ccfg.getName()) || CACHE_NAME_4.equals(ccfg.getName()))
                ccfg.setGroupName(GROUP_2);
        }

        TestCacheGroupTopologyValidatorProvider topValidatorProvider = new TestCacheGroupTopologyValidatorProvider();

        if (isPluginTopValidatorProvider)
            icfg.setPluginProviders(new TestCacheTopologyValidatorPluginProvider(topValidatorProvider));
        else {
            for (CacheConfiguration ccfg : ccfgs)
                ccfg.setTopologyValidator(topValidatorProvider.topologyValidator(ccfg.getGroupName()));
        }

        return icfg.setCacheConfiguration(ccfgs);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Override public void testTopologyValidator() throws Exception {
        startGrid(0);

        putValid(DEFAULT_CACHE_NAME);
        remove(DEFAULT_CACHE_NAME);

        putInvalid(CACHE_NAME_1);
        removeInvalid(CACHE_NAME_1);

        putInvalid(CACHE_NAME_2);
        removeInvalid(CACHE_NAME_2);

        putInvalid(CACHE_NAME_3);
        removeInvalid(CACHE_NAME_3);

        putInvalid(CACHE_NAME_4);
        removeInvalid(CACHE_NAME_4);

        startGrid(1);

        putValid(DEFAULT_CACHE_NAME);
        remove(DEFAULT_CACHE_NAME);

        putValid(CACHE_NAME_1);

        putValid(CACHE_NAME_2);
        remove(CACHE_NAME_2);

        putValid(CACHE_NAME_3);

        putValid(CACHE_NAME_4);
        remove(CACHE_NAME_4);

        startGrid(2);

        putValid(DEFAULT_CACHE_NAME);
        remove(DEFAULT_CACHE_NAME);

        getInvalid(CACHE_NAME_1);
        putInvalid(CACHE_NAME_1);
        removeInvalid(CACHE_NAME_1);

        putInvalid(CACHE_NAME_2);
        removeInvalid(CACHE_NAME_2);

        remove(CACHE_NAME_3);
        putValid(CACHE_NAME_3);

        putValid(CACHE_NAME_4);
        remove(CACHE_NAME_4);
    }

    /** */
    private static class TestCacheGroupTopologyValidatorProvider implements CacheTopologyValidatorProvider, Serializable {
        /** {@inheritDoc} */
        @Override public TopologyValidator topologyValidator(String grpName) {
            if (GROUP_1.equals(grpName)) {
                return new TopologyValidator() {
                    @Override public boolean validate(Collection<ClusterNode> nodes) {
                        return nodes.size() == 2;
                    }
                };
            }
            else if (GROUP_2.equals(grpName)) {
                return new TopologyValidator() {
                    @Override public boolean validate(Collection<ClusterNode> nodes) {
                        return nodes.size() >= 2;
                    }
                };
            }
            else
                return null;
        }
    }
}
