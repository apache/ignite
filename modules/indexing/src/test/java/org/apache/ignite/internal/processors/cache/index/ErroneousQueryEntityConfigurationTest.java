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

package org.apache.ignite.internal.processors.cache.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.Person;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK;

/** Check different variants of erroneous QueryEntity configuration on server and client sides. */
@RunWith(Parameterized.class)
@WithSystemProperty(key = IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK, value = "true")
public class ErroneousQueryEntityConfigurationTest extends AbstractIndexingCommonTest {
    /** Default client name. */
    private static final String CLIENT_NAME = "client";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration igniteCfg = super.getConfiguration(igniteInstanceName);

        if (persistent) {
            igniteCfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(10 * 1024 * 1024)
                )
            );
        }

        return igniteCfg
            .setFailureHandler(new StopNodeFailureHandler());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** */
    @Parameterized.Parameter()
    public Boolean persistent;

    /**
     * @return List of versions pairs to test.
     */
    @Parameterized.Parameters(name = "persistent = {0}")
    public static Collection<Object[]> testData() {
        List<Object[]> res = new ArrayList<>();

        res.add(new Object[] {true});
        res.add(new Object[] {false});

        return res;
    }

    /** Start client node with erroneous configuration, if persistence enabled -
     * check persistent server node is correctly restart. */
    @Test
    public void testErroneousCacheConfigFromClientNode() throws Exception {
        Ignite ignite = startGrid("srv1");

        TcpDiscoverySpi discoConf = (TcpDiscoverySpi)ignite.configuration().getDiscoverySpi();

        TcpDiscoveryIpFinder ipFinder = discoConf.getIpFinder();

        GridTestUtils.assertThrows(log, () -> {
            Ignite client = Ignition.start(getErroneousConfiguration(ipFinder, false));

            client.cluster().state(ClusterState.ACTIVE);
        }, IgniteException.class, "Duplicate index name");

        Ignite client = Ignition.start(getErroneousConfiguration(ipFinder, true));

        client.cluster().state(ClusterState.ACTIVE);

        stopGrid("srv1");

        if (persistent)
            startGrid("srv1");
    }

    /** Try to start client with erroneous cache configs through dynamic caches. */
    @Test
    public void teststartErroneousCacheConfigThroughDynamicCaches() throws Exception {
        IgniteEx server = startGrid();

        IgniteEx client = startClientGrid(CLIENT_NAME);

        Collection<CacheConfiguration> ccfgs = makeMultipleCachesConfig(false);

        client.cluster().state(ClusterState.ACTIVE);

        Throwable th = GridTestUtils.assertThrows(log, () -> {
            client.getOrCreateCaches(ccfgs);
        }, IgniteException.class, null);

        SchemaOperationException e = X.cause(th, SchemaOperationException.class);

        assertEquals(SchemaOperationException.CODE_TABLE_EXISTS, e.code());

        server.cluster().state(ClusterState.ACTIVE);
    }

    /** Check that joining node with already configured index name on different cache will not joined.*/
    @Test
    public void testEqualIndexAreConfiguredOnServerAndJoinedNode() throws Exception {
        IgniteEx server = startGrid("srv1");

        TcpDiscoverySpi discoConf = (TcpDiscoverySpi)server.configuration().getDiscoverySpi();

        TcpDiscoveryIpFinder ipFinder = discoConf.getIpFinder();

        IgniteConfiguration cfg = getErroneousConfiguration(ipFinder, false);

        CacheConfiguration[] ccfgs = cfg.getCacheConfiguration();

        cfg.setCacheConfiguration(ccfgs[0], ccfgs[1]);

        cfg.setClientMode(false);

        cfg.setIgniteInstanceName("srv2");

        startGrid(cfg);

        cfg = getErroneousConfiguration(ipFinder, false);

        cfg.setCacheConfiguration(ccfgs[2], ccfgs[3]);

        cfg.setClientMode(true);

        IgniteConfiguration cfg0 = cfg;

        GridTestUtils.assertThrows(log, () -> {
            IgniteEx client = startGrid(cfg0);

            client.cluster().state(ClusterState.ACTIVE);
        }, IgniteException.class, "Duplicate index name");

        server.cluster().state(ClusterState.ACTIVE);

        assertNotNull(server.cache(ccfgs[0].getName()));

        server.cluster().state(ClusterState.INACTIVE);

        if (persistent) {
            stopGrid("srv1");

            startGrid("srv1");
        }
    }

    /**
     * Creates erroneous cache configurations.
     *
     * @param ipFinder Finder.
     * @param differentSchemas If {@code true} different schemas are used.
     */
    private IgniteConfiguration getErroneousConfiguration(
        TcpDiscoveryIpFinder ipFinder,
        boolean differentSchemas
    ) {
        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();

        discoverySpi.setIpFinder(ipFinder);

        IgniteConfiguration igniteCfg = new IgniteConfiguration();

        igniteCfg.setDiscoverySpi(discoverySpi);

        igniteCfg.setPeerClassLoadingEnabled(true);

        if (persistent) {
            igniteCfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(10 * 1024 * 1024)
                )
            );
        }

        Collection<CacheConfiguration> ccfgs = makeMultipleCachesConfig(differentSchemas);

        igniteCfg.setCacheConfiguration(ccfgs.toArray(new CacheConfiguration[0]));

        igniteCfg.setClientMode(true);

        return igniteCfg;
    }

    /** */
    private static Collection<CacheConfiguration> makeMultipleCachesConfig(boolean differentSchemas) {
        Collection<CacheConfiguration> ccfgs = new ArrayList<>();

        CacheConfiguration<Integer, String> c1 = new CacheConfiguration<>("c1");

        c1.setIndexedTypes(Integer.class, String.class);

        c1.setSqlSchema("TEST_V1");

        CacheConfiguration<Integer, String> c1custom = new CacheConfiguration<>("c1custom");

        c1custom.setIndexedTypes(Integer.class, Person.class);

        c1custom.setSqlSchema("TEST_V1");

        CacheConfiguration<Integer, String> c2 = new CacheConfiguration<>("c2");

        c2.setIndexedTypes(Integer.class, String.class);

        CacheConfiguration<Integer, String> c2custom = new CacheConfiguration<>("c2custom");

        c2custom.setIndexedTypes(Integer.class, Person.class);

        if (differentSchemas) {
            c2.setSqlSchema("TEST_V2");
            c2custom.setSqlSchema("TEST_V2");
        }
        else {
            c2.setSqlSchema("TEST_V1");
            c2custom.setSqlSchema("TEST_V1");
        }

        // appending sequence is important here.
        ccfgs.add(c1);
        ccfgs.add(c1custom);

        ccfgs.add(c2);
        ccfgs.add(c2custom);

        return ccfgs;
    }
}
