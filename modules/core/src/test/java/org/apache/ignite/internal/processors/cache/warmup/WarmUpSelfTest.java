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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WarmUpConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
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
            );
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

        Map<Class<? extends WarmUpConfiguration>, WarmUpStrategy> expStrats =
            Stream.of(new NoOpWarmUp()).collect(toMap(NoOpWarmUp::configClass, identity()));

        Map<Class<? extends WarmUpConfiguration>, WarmUpStrategy> actStrats = CU.warmUpStrategies(n.context());

        assertEquals(expStrats, actStrats);

        stopAllGrids();

        WarmUpTestPluginProvider pluginProvider = new WarmUpTestPluginProvider();

        n = startGrid(getConfiguration(getTestIgniteInstanceName(0)).setPluginProviders(pluginProvider));

        pluginProvider.strats.forEach(strat -> assertNull(expStrats.put(strat.configClass(), strat)));

        actStrats = CU.warmUpStrategies(n.context());

        assertEquals(expStrats, actStrats);
    }
    
    /**
     * Test checks that strategies are executed according to configuration.
     * <p>
     * Steps:
     * 1)Starting a node with a single region that has been configured for {@link SimpleObservableWarmUp};
     * 2)Check that strategy was executed only for it region;
     * 3)Restarting node with default {@link SimpleObservableWarmUpConfiguration};
     * 4)Checks that {@link SimpleObservableWarmUp} was only executed for persistent regions
     * that were not configured by {@link SimpleObservableWarmUpConfiguration}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExecutionStrategies() throws Exception {
        WarmUpTestPluginProvider pluginProvider = new WarmUpTestPluginProvider();

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0))
            .setPluginProviders(pluginProvider)
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

        SimpleObservableWarmUp observableWarmUp = (SimpleObservableWarmUp)pluginProvider.strats.get(0);

        assertEquals(1, observableWarmUp.visitRegions.size());
        assertTrue(observableWarmUp.visitRegions.containsKey("2"));
        assertEquals(1, observableWarmUp.visitRegions.get("2").get());

        stopAllGrids();

        cfg = getConfiguration(getTestIgniteInstanceName(0))
            .setPluginProviders((pluginProvider = new WarmUpTestPluginProvider()))
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

        observableWarmUp = (SimpleObservableWarmUp)pluginProvider.strats.get(0);

        assertEquals(2, observableWarmUp.visitRegions.size());

        assertTrue(observableWarmUp.visitRegions.containsKey("1"));
        assertTrue(observableWarmUp.visitRegions.containsKey("3"));

        assertEquals(1, observableWarmUp.visitRegions.get("1").get());
        assertEquals(1, observableWarmUp.visitRegions.get("3").get());
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
     * Test plugin provider for test strategies.
     */
    private static class WarmUpTestPluginProvider extends AbstractTestPluginProvider {
        /** Collection of strategies. */
        private final List<WarmUpStrategy<?>> strats = Arrays.asList(new SimpleObservableWarmUp());

        /** {@inheritDoc} */
        @Override public String name() {
            return getClass().getSimpleName();
        }

        /** {@inheritDoc} */
        @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
            super.initExtensions(ctx, registry);

            registry.registerExtension(WarmUpStrategySupplier.class, new WarmUpStrategySupplier() {
                /** {@inheritDoc} */
                @Override public Collection<WarmUpStrategy<?>> strategies() {
                    return strats;
                }
            });
        }
    }

    /**
     * Simple observable warm-up configuration.
     */
    private static class SimpleObservableWarmUpConfiguration implements WarmUpConfiguration {
        // No-op.
    }

    /**
     * Simple observable warm-up strategy.
     */
    private static class SimpleObservableWarmUp implements WarmUpStrategy<SimpleObservableWarmUpConfiguration> {
        /** Visited regions with a counter. */
        private final Map<String, AtomicInteger> visitRegions = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override public Class<SimpleObservableWarmUpConfiguration> configClass() {
            return SimpleObservableWarmUpConfiguration.class;
        }

        /** {@inheritDoc} */
        @Override public void warmUp(
            GridKernalContext kernalCtx,
            SimpleObservableWarmUpConfiguration cfg,
            DataRegion region
        ) throws IgniteCheckedException {
            visitRegions.computeIfAbsent(region.config().getName(), s -> new AtomicInteger()).incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void close() throws IgniteCheckedException {
            // No-op.
        }
    }
}
