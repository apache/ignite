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

package org.apache.ignite.internal.processors.configuration.distributed;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test default values configuration for distributed properties.
 */
public class DistributedConfigurationDefaultValuesTest extends GridCommonAbstractTest {
    /** */
    private Consumer<DistributedPropertyDispatcher> onReadyToRegister;

    /** */
    private Runnable onReadyToWrite;

    /** */
    Map<String, String> dfltPropVals;

    /** */
    private boolean pds;

    /** */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(listeningLog);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setPersistenceEnabled(pds)
        ));

        cfg.setDistributedPropertiesDefaultValues(dfltPropVals);

        cfg.setPluginProviders(new AbstractTestPluginProvider() {
            @Override public String name() {
                return "Distributed property register";
            }

            @Override public void start(PluginContext ctx) {
                ((IgniteEx)ctx.grid()).context().internalSubscriptionProcessor().getDistributedConfigurationListeners().add(
                    new DistributedConfigurationLifecycleListener() {
                        @Override public void onReadyToRegister(DistributedPropertyDispatcher dispatcher) {
                            if (onReadyToRegister != null)
                                onReadyToRegister.accept(dispatcher);
                        }

                        @Override public void onReadyToWrite() {
                            if (onReadyToWrite != null)
                                onReadyToWrite.run();
                        }
                    }
                );
            }
        });

        return cfg;
    }

    /** */
    @Test
    public void testDifferentPropertyTypes() throws Exception {
        DistributedLongProperty longProp = DistributedLongProperty.detachedLongProperty("longProp", "");
        DistributedBooleanProperty boolProp = DistributedBooleanProperty.detachedBooleanProperty("boolProp", "");
        SimpleDistributedProperty<String> strProp = new SimpleDistributedProperty<>("stringProp", Function.identity(), "");
        DistributedEnumProperty<ClusterState> enumProp = new DistributedEnumProperty<>(
            "enumProp", "",
            ordinal -> ordinal == null ? null : ClusterState.fromOrdinal(ordinal),
            state -> state == null ? null : state.ordinal(),
            ClusterState.class
        );

        dfltPropVals = Map.of(
            "longProp", "1",
            "boolProp", "true",
            "stringProp", "val",
            "enumProp", "ACTIVE"
        );

        onReadyToRegister = dispatcher -> dispatcher.registerProperties(longProp, boolProp, strProp, enumProp);

        Map<String, String> lsnrProps = new HashMap<>();

        DistributePropertyListener<Object> lsnr = (name, oldVal, newVal) -> {
            if (newVal != null)
                lsnrProps.put(name, newVal.toString());
        };

        longProp.addListener(lsnr);
        boolProp.addListener(lsnr);
        strProp.addListener(lsnr);
        enumProp.addListener(lsnr);

        // Properties values when distributed configuration is ready to write.
        Map<String, String> lsnrProps0 = new HashMap<>();

        onReadyToWrite = () -> lsnrProps0.putAll(lsnrProps);

        startGrid(0);

        assertEquals(dfltPropVals, lsnrProps0);

        assertEquals((Long)1L, longProp.get());
        assertEquals(Boolean.TRUE, boolProp.get());
        assertEquals("val", strProp.get());
        assertEquals(ClusterState.ACTIVE, enumProp.get());
    }

    /** */
    @Test
    public void testInMemoryCluster() throws Exception {
        checkDistributedPropertiesClusterPropagation(false);
    }

    /** */
    @Test
    public void testPersistentCluster() throws Exception {
        checkDistributedPropertiesClusterPropagation(true);
    }

    /** */
    private void checkDistributedPropertiesClusterPropagation(boolean pds) throws Exception {
        this.pds = pds;

        String propName = "testProp";

        // Properties with the same name, but on different nodes.
        DistributedLongProperty prop0 = DistributedLongProperty.detachedLongProperty(propName, "");
        DistributedLongProperty prop1 = DistributedLongProperty.detachedLongProperty(propName, "");
        DistributedLongProperty prop2 = DistributedLongProperty.detachedLongProperty(propName, "");

        dfltPropVals = F.asMap(propName, "1");

        onReadyToRegister = dispatcher -> dispatcher.registerProperties(prop0);

        startGrid(0).cluster().state(ClusterState.ACTIVE);

        assertEquals("Expecting value from default configuration", (Long)1L, prop0.get());

        dfltPropVals = null;

        onReadyToRegister = dispatcher -> dispatcher.registerProperties(prop1);

        startGrid(1);

        assertEquals("Expecting value from cluster, when value from cluster is not empty " +
            "and default configuration is empty", (Long)1L, prop1.get());

        dfltPropVals = F.asMap(propName, "2");

        onReadyToRegister = dispatcher -> dispatcher.registerProperties(prop2);

        startGrid(2);

        assertEquals("Expecting value from cluster, when value from cluster and default configuration " +
            "is not empty", (Long)1L, prop2.get());

        if (!pds) // Further checks for PDS after cluster restart.
            return;

        stopAllGrids();

        // Properties for all nodes after restart.
        DistributedLongProperty prop3 = DistributedLongProperty.detachedLongProperty(propName, "");
        DistributedLongProperty prop4 = DistributedLongProperty.detachedLongProperty(propName, "");
        DistributedLongProperty prop5 = DistributedLongProperty.detachedLongProperty(propName, "");

        dfltPropVals = F.asMap(propName, "2");

        onReadyToRegister = dispatcher -> dispatcher.registerProperties(prop3);

        startGrid(0);

        assertEquals("Expecting value from PDS, when default configuration is not empty and PDS is not empty", (Long)1L, prop3.get());

        prop3.propagateAsync(2L).get();

        dfltPropVals = null;

        onReadyToRegister = dispatcher -> dispatcher.registerProperties(prop4);

        startGrid(1);

        assertEquals("Expecting value from cluster, when value from cluster is not empty, default configuration " +
            "is empty and PDS is not empty", (Long)2L, prop4.get());

        dfltPropVals = F.asMap(propName, "3");

        onReadyToRegister = dispatcher -> dispatcher.registerProperties(prop5);

        startGrid(2);

        assertEquals("Expecting value from cluster, when value from cluster is not empty, default configuration " +
            "is not empty and PDS is not empty", (Long)2L, prop5.get());
    }

    /** */
    @Test
    public void testPropertyInitBySecondNode() throws Exception {
        String propName = "testProp";

        DistributedLongProperty prop0 = DistributedLongProperty.detachedLongProperty(propName, "");
        DistributedLongProperty prop1 = DistributedLongProperty.detachedLongProperty(propName, "");

        onReadyToRegister = dispatcher -> dispatcher.registerProperties(prop0);

        startGrid(0);

        assertNull(prop0.get());

        dfltPropVals = F.asMap(propName, "1");

        onReadyToRegister = dispatcher -> dispatcher.registerProperties(prop1);

        startGrid(1);

        assertEquals((Long)1L, prop1.get());
        assertEquals((Long)1L, prop0.get());
    }

    /** */
    @Test
    public void testNotRegisteredProperty() throws Exception {
        String propName = "testProp";
        String notExistingPropName = "notExistingProp";

        DistributedLongProperty prop = DistributedLongProperty.detachedLongProperty(propName, "");

        dfltPropVals = F.asMap(propName, "1", notExistingPropName, "2");

        onReadyToRegister = dispatcher -> dispatcher.registerProperties(prop);

        String warn = "Cannot set default value for distributed property 'notExistingProp', property is not registered";

        LogListener logLsnr = LogListener.matches(warn).build();

        listeningLog.registerListener(logLsnr);

        startGrid(0);

        assertEquals((Long)1L, prop.get());
        assertTrue(logLsnr.check());
    }
}
