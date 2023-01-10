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

package org.apache.ignite.internal.processors.cache.warmup;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NoOpWarmUpConfiguration;
import org.apache.ignite.configuration.WarmUpConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.client.IgniteClientInternal;
import org.apache.ignite.internal.client.thin.TcpIgniteClient;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.mxbean.WarmUpMXBean;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.client.Config.SERVER;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/**
 * Class for testing warm-up.
 */
public class WarmUpSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDataRegionConfigurations(
                        new DataRegionConfiguration()
                            .setName("persist")
                            .setPersistenceEnabled(true)
                            .setWarmUpConfiguration(new NoOpWarmUpConfiguration())
                    )
            ).setPluginProviders(new WarmUpTestPluginProvider());
    }

    /**
     * Test checks that an unknown default warm-up configuration cannot be passed.
     * <p>
     * Steps:
     * 1)Adding an unknown warm-up configuration to {@link DataStorageConfiguration};
     * 2)Starting node and getting an error.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUnknownDefaultWarmUpConfiguration() throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0))
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultWarmUpConfiguration(new WarmUpConfiguration() {
                    })
            );

        assertThrowsAnyCause(
            log,
            () -> startGrid(cfg),
            IgniteCheckedException.class,
            "Unknown default warm-up configuration"
        );
    }

    /**
     * Test checks that an unknown data region warm-up configuration cannot be passed.
     * <p>
     * Steps:
     * 1)Adding an unknown warm-up configuration to {@link DataRegionConfiguration};
     * 2)Starting node and getting an error.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUnknownDataRegionWarmUpConfiguration() throws Exception {
        String regName = "error";

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0))
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDataRegionConfigurations(
                        new DataRegionConfiguration()
                            .setName(regName)
                            .setPersistenceEnabled(true)
                            .setWarmUpConfiguration(new WarmUpConfiguration() {
                            })
                    )
            );

        assertThrowsAnyCause(
            log,
            () -> startGrid(cfg),
            IgniteCheckedException.class,
            "Unknown data region warm-up configuration: [name=" + regName
        );
    }

    /**
     * Test checks that an unknown data region warm-up configuration cannot be passed.
     * <p>
     * Steps:
     * 1)Adding an warm-up configuration to non-persistent {@link DataRegionConfiguration};
     * 2)Starting node and getting an error.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNonPersistentDataRegionWarmUpConfiguration() throws Exception {
        String regName = "error";

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0))
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDataRegionConfigurations(
                        new DataRegionConfiguration()
                            .setName(regName)
                            .setWarmUpConfiguration(new NoOpWarmUpConfiguration())
                    )
            );

        assertThrowsAnyCause(
            log,
            () -> startGrid(cfg),
            IgniteCheckedException.class,
            "Warm-up setting is not expected for a non-persistent data region: [name=" + regName
        );
    }

    /**
     * Test verifies that available warm-up strategies are correct.
     * <p>
     * Steps:
     * 1)Starting a node, without plugins;
     * 2)Check that only basic strategies are available;
     * 3)Restarting a node with a test plugin containing additional strategies;
     * 4)Checking that basic + from plugin strategies are available.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAvailableWarmUpStrategies() throws Exception {
        IgniteEx n = startGrid(getConfiguration(getTestIgniteInstanceName(0)).setPluginProviders());

        GridCacheProcessor cacheProc = n.context().cache();

        Map<Class<? extends WarmUpConfiguration>, WarmUpStrategy> expStrats =
            Stream.of(new NoOpWarmUpStrategy(), new LoadAllWarmUpStrategy(log, cacheProc::cacheGroups))
                .collect(toMap(WarmUpStrategy::configClass, identity()));

        Map<Class<? extends WarmUpConfiguration>, WarmUpStrategy> actStrats = CU.warmUpStrategies(n.context());

        assertEquals(expStrats, actStrats);

        stopAllGrids();

        n = startGrid(0);

        WarmUpTestPluginProvider pluginProvider = (WarmUpTestPluginProvider)n.configuration().getPluginProviders()[0];

        pluginProvider.strats.forEach(strat -> assertNull(expStrats.put(strat.configClass(), strat)));

        actStrats = CU.warmUpStrategies(n.context());

        assertEquals(expStrats, actStrats);
    }
    
    /**
     * Test checks that strategies are executed according to configuration.
     * <p>
     * Steps:
     * 1)Starting a node with a single region that has been configured for {@link SimpleObservableWarmUpStrategy};
     * 2)Check that strategy was executed only for it region;
     * 3)Restarting node with default {@link SimpleObservableWarmUpConfiguration};
     * 4)Checks that {@link SimpleObservableWarmUpStrategy} was only executed for persistent regions
     * that were not configured by {@link SimpleObservableWarmUpConfiguration}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExecutionStrategies() throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0))
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDataRegionConfigurations(
                        new DataRegionConfiguration().setName("0"),
                        new DataRegionConfiguration().setName("1").setPersistenceEnabled(true),
                        new DataRegionConfiguration().setName("2").setPersistenceEnabled(true)
                            .setWarmUpConfiguration(new SimpleObservableWarmUpConfiguration())
                    )
            );

        startGrid(cfg);

        WarmUpTestPluginProvider pluginProvider = (WarmUpTestPluginProvider)cfg.getPluginProviders()[0];
        SimpleObservableWarmUpStrategy observableWarmUp = (SimpleObservableWarmUpStrategy)pluginProvider.strats.get(0);

        assertEquals(1, observableWarmUp.visitRegions.size());
        assertTrue(observableWarmUp.visitRegions.containsKey("2"));
        assertEquals(1, observableWarmUp.visitRegions.get("2").get());

        stopAllGrids();

        cfg = getConfiguration(getTestIgniteInstanceName(0))
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultWarmUpConfiguration(new SimpleObservableWarmUpConfiguration())
                    .setDataRegionConfigurations(
                        new DataRegionConfiguration().setName("0"),
                        new DataRegionConfiguration().setName("1").setPersistenceEnabled(true),
                        new DataRegionConfiguration().setName("2").setPersistenceEnabled(true)
                            .setWarmUpConfiguration(new NoOpWarmUpConfiguration())
                    ).setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setName("3").setPersistenceEnabled(true)
                    )
            );

        startGrid(cfg);

        pluginProvider = (WarmUpTestPluginProvider)cfg.getPluginProviders()[0];
        observableWarmUp = (SimpleObservableWarmUpStrategy)pluginProvider.strats.get(0);

        assertEquals(2, observableWarmUp.visitRegions.size());

        assertTrue(observableWarmUp.visitRegions.containsKey("1"));
        assertTrue(observableWarmUp.visitRegions.containsKey("3"));

        assertEquals(1, observableWarmUp.visitRegions.get("1").get());
        assertEquals(1, observableWarmUp.visitRegions.get("3").get());
    }

    /**
     * Test checks to stop warming up.
     * <p>
     * Steps:
     * 1)Running a node in a separate thread with {@link BlockedWarmUpConfiguration} for one region;
     * 2)Stop warm-up;
     * 3)Make sure that warm-up is stopped and node has started successfully.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStopWarmUp() throws Exception {
        checkStopWarmUp(new IgniteInClosureX<IgniteKernal>() {
            /** {@inheritDoc} */
            @Override public void applyx(IgniteKernal kernal) throws IgniteCheckedException {
                assertTrue(kernal.context().cache().stopWarmUp());
                assertFalse(kernal.context().cache().stopWarmUp());
            }
        });
    }

    /**
     * Test checks to stop warming up by MXBean.
     * <p>
     * Steps:
     * 1)Running a node in a separate thread with {@link BlockedWarmUpConfiguration} for one region;
     * 2)Stop warm-up by MXBean;
     * 3)Make sure that warm-up is stopped and node has started successfully.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStopWarmUpByMXBean() throws Exception {
        checkStopWarmUp(new IgniteInClosureX<IgniteKernal>() {
            /** {@inheritDoc} */
            @Override public void applyx(IgniteKernal kernal) throws IgniteCheckedException {
                WarmUpMXBean warmUpMXBean = getMxBean(
                    kernal.configuration().getIgniteInstanceName(),
                    "WarmUp",
                    WarmUpMXBeanImpl.class.getSimpleName(),
                    WarmUpMXBean.class
                );

                warmUpMXBean.stopWarmUp();
            }
        });
    }

    /**
     * Test checks to stop warming up by {@link org.apache.ignite.internal.client.IgniteClientInternal}.
     * <p>
     * Steps:
     * 1)Running a node in a separate thread with {@link BlockedWarmUpConfiguration} for one region;
     * 2)Stop warm-up by {@link org.apache.ignite.internal.client.IgniteClientInternal};
     * 3)Make sure that warm-up is stopped and node has started successfully.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStopWarmUpByThinClient() throws Exception {
        checkStopWarmUp(new IgniteInClosureX<IgniteKernal>() {
            /** {@inheritDoc} */
            @Override public void applyx(IgniteKernal kernal) {
                try (IgniteClientInternal thinCli =
                    (IgniteClientInternal)TcpIgniteClient.start(new ClientConfiguration().setAddresses(SERVER))) {

                    thinCli.stopWarmUp();
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    /**
     * Asserts that two warm-up maps are equal.
     *
     * @param exp Expected value.
     * @param act Actual value
     */
    private void assertEquals(
        Map<Class<? extends WarmUpConfiguration>, WarmUpStrategy> exp,
        Map<Class<? extends WarmUpConfiguration>, WarmUpStrategy> act
    ) {
        assertEquals(exp.size(), act.size());

        exp.forEach((cfgCls, strat) -> {
            assertTrue(cfgCls.toString(), act.containsKey(cfgCls));
            assertTrue(cfgCls.toString(), act.get(cfgCls).getClass() == strat.getClass());
        });
    }

    /**
     * Checking correct warm-up stop.
     *
     * @param stopCX Stop warm-up function.
     * @throws Exception If failed.
     */
    private void checkStopWarmUp(IgniteInClosureX<IgniteKernal> stopCX) throws Exception {
        requireNonNull(stopCX);

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0))
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDataRegionConfigurations(
                        new DataRegionConfiguration().setName("1").setPersistenceEnabled(true)
                            .setWarmUpConfiguration(new BlockedWarmUpConfiguration())
                    )
            );

        IgniteInternalFuture<IgniteEx> stratFut = GridTestUtils.runAsync(() -> startGrid(cfg));

        WarmUpTestPluginProvider pluginProvider = (WarmUpTestPluginProvider)cfg.getPluginProviders()[0];
        BlockedWarmUpStrategy strat = (BlockedWarmUpStrategy)pluginProvider.strats.get(1);

        strat.startLatch.await(10, TimeUnit.SECONDS);

        IgniteKernal n = IgnitionEx.gridx(cfg.getIgniteInstanceName());

        stopCX.apply(n);

        assertEquals(0, strat.stopLatch.getCount());
        assertNotNull(stratFut.get(TimeUnit.MINUTES.toMillis(1)));
    }
}
