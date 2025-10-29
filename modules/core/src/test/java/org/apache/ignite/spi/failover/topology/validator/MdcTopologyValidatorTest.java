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

package org.apache.ignite.spi.failover.topology.validator;

import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.topology.MdcTopologyValidator;
import org.junit.Test;

/** */
public class MdcTopologyValidatorTest extends GridCommonAbstractTest {
    /** */
    private static final String DC_ID_0 = "DC0";

    /** */
    private static final String DC_ID_1 = "DC1";

    /** */
    private static final String DC_ID_2 = "DC2";

    /** */
    private static final String KEY = "key";

    /** */
    private static final String VAL = "val";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();

        System.clearProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                    )
            )
            .setConsistentId(igniteInstanceName);
    }

    /** */
    @Test
    public void testEmptyConfig() {
        MdcTopologyValidator topValidator = new MdcTopologyValidator();

        GridTestUtils.assertThrows(log,
            () -> createClusterWithCache(topValidator, false),
            CacheException.class,
            "Missing datacenters or main datacenter");
    }

    /** */
    @Test
    public void testOddDcsWithMain() {
        MdcTopologyValidator topValidator = new MdcTopologyValidator();

        topValidator.setDatacenters(Set.of(DC_ID_0, DC_ID_1, DC_ID_2));
        topValidator.setMainDatacenter(DC_ID_1);

        GridTestUtils.assertThrows(log,
            () -> createClusterWithCache(topValidator, false),
            CacheException.class,
            "Datacenters count must be even when main datacenter is set.");
    }

    /** Checks 1DC case with MdcTopologyValidator usage.*/
    @Test
    public void testEmptyDc() {
        MdcTopologyValidator topValidator = new MdcTopologyValidator();

        topValidator.setDatacenters(Set.of());

        GridTestUtils.assertThrows(log,
            () -> createClusterWithCache(topValidator, false),
            CacheException.class,
            "Missing datacenters");
    }

    /** */
    @Test
    public void testNodeWithoutDcSpecifiedWithMajorityBasedValidator() throws Exception {
        MdcTopologyValidator topValidator = new MdcTopologyValidator();

        topValidator.setDatacenters(Set.of(DC_ID_0, DC_ID_1, DC_ID_2));

        IgniteEx srv = startGrid(0);

        waitForTopology(1);

        srv.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> cfgCache = new CacheConfiguration<>("cache").setTopologyValidator(topValidator);

        IgniteCache cache = srv.createCache(cfgCache);

        GridTestUtils.assertThrows(log, () -> cache.put(KEY, VAL), IgniteException.class, "cache topology is not valid");
    }

    /** */
    @Test
    public void testNodeWithoutDcSpecifiedWithMainBasedValidator() throws Exception {
        MdcTopologyValidator topValidator = new MdcTopologyValidator();

        topValidator.setMainDatacenter(DC_ID_1);

        IgniteEx srv = startGrid(0);

        waitForTopology(1);

        srv.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> cfgCache = new CacheConfiguration<>("cache").setTopologyValidator(topValidator);

        IgniteCache cache = srv.createCache(cfgCache);

        GridTestUtils.assertThrows(log, () -> cache.put(KEY, VAL), IgniteException.class, "cache topology is not valid");
    }

    /** */
    @Test
    public void testClientDoesNotAffectValidation() throws Exception {
        MdcTopologyValidator topValidator = new MdcTopologyValidator();

        topValidator.setMainDatacenter(DC_ID_1);
        topValidator.setDatacenters(Set.of(DC_ID_0, DC_ID_1));

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_0);
        startGrid(0);

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_1);
        IgniteEx client = startClientGrid("client");

        waitForTopology(2);

        client.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> cfgCache = new CacheConfiguration<>("cache").setTopologyValidator(topValidator);

        IgniteCache<Object, Object> cache = client.getOrCreateCache(cfgCache);

        GridTestUtils.assertThrows(log, () -> cache.put(KEY, VAL), IgniteException.class, "cache topology is not valid");
    }

    /** */
    @Test
    public void testTopologyValidatorEqualityCheck() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_1);
        IgniteEx srv0 = startGrid(0);

        startGrid(1);

        waitForTopology(2);

        srv0.cluster().state(ClusterState.ACTIVE);

        MdcTopologyValidator topValidator = new MdcTopologyValidator();

        topValidator.setMainDatacenter(DC_ID_1);
        topValidator.setDatacenters(Set.of(DC_ID_0, DC_ID_1));

        CacheConfiguration<Object, Object> cfgCache1 = new CacheConfiguration<>(DEFAULT_CACHE_NAME).setTopologyValidator(topValidator);

        srv0.getOrCreateCache(cfgCache1);

        stopGrid(1);

        srv0.destroyCache(cfgCache1.getName());

        topValidator.setMainDatacenter(DC_ID_0); // Changed

        CacheConfiguration<Object, Object> cfgCache2 = new CacheConfiguration<>(DEFAULT_CACHE_NAME).setTopologyValidator(topValidator);

        srv0.getOrCreateCache(cfgCache2);

        startGrid(2);

        waitForTopology(2);

        GridTestUtils.assertThrows(log, () -> startGrid(1), IgniteCheckedException.class, "Cache topology validator mismatch");
    }

    /** */
    @Test
    public void testMainDcBasedValidator() throws Exception {
        MdcTopologyValidator topValidator = new MdcTopologyValidator();

        topValidator.setDatacenters(Set.of(DC_ID_0, DC_ID_1, DC_ID_2, "DC4"));
        topValidator.setMainDatacenter(DC_ID_1);

        IgniteCache<Object, Object> cache = createClusterWithCache(topValidator, true);

        cache.put(KEY, VAL);
        assertEquals(VAL, cache.get(KEY));

        stopGrid(2);

        cache.put(KEY, VAL + 1);
        assertEquals(VAL + 1, cache.get(KEY));

        stopGrid(1);

        GridTestUtils.assertThrows(log, () -> cache.put(KEY, VAL + 2), IgniteException.class, "cache topology is not valid");
    }

    /** */
    @Test
    public void testMajority() throws Exception {
        MdcTopologyValidator topValidator = new MdcTopologyValidator();

        topValidator.setDatacenters(Set.of(DC_ID_0, DC_ID_1, DC_ID_2));

        IgniteCache<Object, Object> cache = createClusterWithCache(topValidator, true);

        cache.put(KEY, VAL);
        assertEquals(VAL, cache.get(KEY));

        int randomNode = ThreadLocalRandom.current().nextInt(1, 3);

        stopGrid(randomNode);

        cache.put(KEY, VAL + 1);
        assertEquals(VAL + 1, cache.get(KEY));

        stopGrid(randomNode == 1 ? 2 : 1);

        GridTestUtils.assertThrows(log, () -> cache.put(KEY, VAL + 2), IgniteException.class, "cache topology is not valid");
    }

    /** */
    @Test
    public void testBigCluster() throws Exception {
        MdcTopologyValidator topValidator = new MdcTopologyValidator();

        topValidator.setDatacenters(Set.of(DC_ID_0, DC_ID_1, DC_ID_2));

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_0);

        startGrid(0);

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_1);

        IgniteEx srv = startGrid(1);
        startGrid(10);
        startGrid(11);
        startGrid(12);

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_2);

        startGrid(2);

        waitForTopology(6);

        srv.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> cfgCache = new CacheConfiguration<>("cache").setTopologyValidator(topValidator);

        IgniteCache<Object, Object> cache = srv.createCache(cfgCache);

        cache.put(KEY, VAL);
        assertEquals(VAL, cache.get(KEY));

        stopGrid(0);
        stopGrid(2);

        // Checking case when 4 nodes are alive, but only in single DC
        GridTestUtils.assertThrows(log, () -> cache.put(KEY, VAL + 2), IgniteException.class, "cache topology is not valid");
    }

    /** */
    private IgniteCache<Object, Object> createClusterWithCache(TopologyValidator topValidator, boolean setDc) throws Exception {
        if (setDc)
            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_0);

        IgniteEx srv0 = startGrid(0);

        if (setDc)
            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_1);

        startGrid(1);

        if (setDc)
            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_2);

        startGrid(2);

        waitForTopology(3);

        srv0.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> cfgCache = new CacheConfiguration<>("cache").setTopologyValidator(topValidator);

        return srv0.createCache(cfgCache);
    }
}
