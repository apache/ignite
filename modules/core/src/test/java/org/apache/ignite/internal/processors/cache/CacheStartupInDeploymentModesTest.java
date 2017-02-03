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
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test verifies that it's possible to start caches in Isolated and Private mode when BinaryMarshaller is used.
 */
public class CacheStartupInDeploymentModesTest extends GridCommonAbstractTest {
    /** */
    private static final String REPLICATED_CACHE = "replicated";

    /** */
    private static final String PARTITIONED_CACHE = "partitioned";

    /** */
    private DeploymentMode deploymentMode;

    /** */
    private Marshaller marshaller;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(marshaller);
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

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If fail.
     */
    public void testFailedInIsolatedMode() throws Exception {
        deploymentMode = DeploymentMode.ISOLATED;
        marshaller = new OptimizedMarshaller();

        doCheckFailed();
    }

    /**
     * @throws Exception If fail.
     */
    public void testFailedInPrivateMode() throws Exception {
        deploymentMode = DeploymentMode.PRIVATE;
        marshaller = new OptimizedMarshaller();

        doCheckFailed();
    }

    /**
     * @throws Exception If fail.
     */
    public void testStartedInIsolatedMode() throws Exception {
        deploymentMode = DeploymentMode.ISOLATED;
        marshaller = new BinaryMarshaller();

        doCheckStarted(deploymentMode);
    }

    /**
     * @throws Exception If fail.
     */
    public void testStartedInPrivateMode() throws Exception {
        deploymentMode = DeploymentMode.PRIVATE;
        marshaller = new BinaryMarshaller();

        doCheckStarted(deploymentMode);
    }

    /**
     * @param mode Deployment mode.
     * @throws Exception If failed.
     */
    private void doCheckStarted(DeploymentMode mode) throws Exception {
        startGridsMultiThreaded(2);

        checkTopology(2);

        assertEquals(mode, ignite(0).configuration().getDeploymentMode());

        assert ignite(0).configuration().getMarshaller() instanceof BinaryMarshaller;

        IgniteCache rCache = ignite(0).cache(REPLICATED_CACHE);

        checkPutCache(rCache);

        IgniteCache pCache = ignite(0).cache(PARTITIONED_CACHE);

        checkPutCache(pCache);
    }

    /**
     * @throws Exception If failed.
     */
    private void doCheckFailed() throws Exception {
        try {
            startGridsMultiThreaded(2);
        }
        catch (Exception e) {
            assert e.getMessage().contains("Cache can be started in PRIVATE or ISOLATED deployment mode only ");

            return;
        }

        fail("Unexpected start of the caches!");
    }

    /**
     * @param cache IgniteCache
     */
    private void checkPutCache(IgniteCache cache) {
        for (int i = 0; i < 10; i++) {
            Organization val = new Organization();

            val.setId(i);
            val.setName("Org " + i);

            cache.put(i, val);
        }

        for (int i = 0; i < 10; i++) {
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
