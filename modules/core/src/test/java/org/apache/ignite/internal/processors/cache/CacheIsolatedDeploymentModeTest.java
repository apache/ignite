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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test is verifying cache start in isolated mode with BinaryMarshaller.
 */
public class CacheIsolatedDeploymentModeTest extends GridCommonAbstractTest {

    private static final String REPLICATED_CACHE = "replicated";
    private static final String PARTITIONED_CACHE = "partitioned";

    private DeploymentMode deploymentMode = DeploymentMode.ISOLATED;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new BinaryMarshaller());
        cfg.setDeploymentMode(deploymentMode);

        CacheConfiguration cacheCfg1 = new CacheConfiguration();
        cacheCfg1.setCacheMode(CacheMode.REPLICATED);
        cacheCfg1.setName(REPLICATED_CACHE);

        CacheConfiguration cacheCfg2 = new CacheConfiguration();
        cacheCfg2.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg2.setName(PARTITIONED_CACHE);

        cfg.setCacheConfiguration(cacheCfg1, cacheCfg2);

        return cfg;
    }

    /**
     * Test append and get elements to cache in ISOLATED mode.
     * @throws Exception If fail.
     */
    public void testCacheIsolated() throws Exception {
        deploymentMode = DeploymentMode.ISOLATED;
        try {
            startGridsMultiThreaded(2);

            checkTopology(2);

            assertEquals(DeploymentMode.ISOLATED, ignite(0).configuration().getDeploymentMode());

            assert ignite(0).configuration().getMarshaller() instanceof BinaryMarshaller;

            IgniteCache rCache = ignite(0).cache(REPLICATED_CACHE);

            checkPutCache(rCache);

            IgniteCache pCache = ignite(0).cache(PARTITIONED_CACHE);

            checkPutCache(pCache);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test append and get elements to cache in PRIVATE mode.
     * @throws Exception If fail.
     */
    public void testCachePrivate() throws Exception {
        deploymentMode = DeploymentMode.PRIVATE;
        try {
            startGridsMultiThreaded(2);

            checkTopology(2);

            assertEquals(DeploymentMode.PRIVATE, ignite(0).configuration().getDeploymentMode());

            assert ignite(0).configuration().getMarshaller() instanceof BinaryMarshaller;

            IgniteCache rCache = ignite(0).cache(REPLICATED_CACHE);

            checkPutCache(rCache);

            IgniteCache pCache = ignite(0).cache(PARTITIONED_CACHE);

            checkPutCache(pCache);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param cache Ignite cache.
     */
    private void checkPutCache(IgniteCache cache) {
        for (int i=0; i<10; i++) {
            Organization val = new Organization();
            val.setId(i);
            val.setName("Org " + i);
            cache.put(i, val);
        }

        for (int i=0; i<10; i++) {
            Organization org = (Organization)cache.get(i);
            assertEquals(i, org.getId());
        }
    }

    /**
     * Cache value class.
     */
    private static class Organization {

        /**
         * Identifier.
         */
        private int id;

        /**
         * Name.
         */
        private String name;

        /**
         * Default constructor.
         */
        public Organization() {
        }

        /**
         * @return Identifier.
         */
        public int getId() {
            return id;
        }

        /**
         * @param id Identifier.
         */
        public void setId(int id) {
            this.id = id;
        }

        /**
         * @return Name.
         */
        public String getName() {
            return name;
        }

        /**
         * @param name Name.
         */
        public void setName(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Organization{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
        }

    }
}
