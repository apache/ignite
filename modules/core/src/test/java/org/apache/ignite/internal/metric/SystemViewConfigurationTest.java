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

package org.apache.ignite.internal.metric;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.SensitiveProperty;
import org.apache.ignite.internal.managers.systemview.IgniteConfigurationIterable;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.spi.systemview.view.ConfigurationView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CONSISTENCY_VIOLATION;
import static org.apache.ignite.internal.IgniteKernal.CFG_VIEW;
import static org.apache.ignite.internal.util.IgniteUtils.MB;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.sslFactory;

/** Tests for {@link SystemView} for configuration. */
public class SystemViewConfigurationTest extends SystemViewAbstractTest {
    /** */
    @Test
    public void testConfigurationView() throws Exception {
        long expMaxSize = 10 * MB;

        String expName = "my-instance";

        String expDrName = "my-dr";

        IgniteConfiguration icfg = getConfiguration(expName)
            .setIncludeEventTypes(EVT_CONSISTENCY_VIOLATION)
            .setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setLazyMemoryAllocation(false))
            .setDataRegionConfigurations(
                new DataRegionConfiguration()
                    .setName(expDrName)
                    .setMaxSize(expMaxSize)));

        try (IgniteEx ignite = startGrid(icfg)) {
            Map<String, String> viewContent = new HashMap<>();

            ignite.context().systemView().<ConfigurationView>view(CFG_VIEW)
                .forEach(view -> viewContent.put(view.name(), view.value()));

            assertEquals(expName, viewContent.get("IgniteInstanceName"));
            assertEquals(
                "false",
                viewContent.get("DataStorageConfiguration.DefaultDataRegionConfiguration.LazyMemoryAllocation")
            );
            assertEquals(expDrName, viewContent.get("DataStorageConfiguration.DataRegionConfigurations[0].Name"));
            assertEquals(
                Long.toString(expMaxSize),
                viewContent.get("DataStorageConfiguration.DataRegionConfigurations[0].MaxSize")
            );
            assertEquals(
                CacheAtomicityMode.TRANSACTIONAL.name(),
                viewContent.get("CacheConfiguration[0].AtomicityMode")
            );
            assertTrue(viewContent.containsKey("AddressResolver"));
            assertNull(viewContent.get("AddressResolver"));
            assertEquals("[" + EVT_CONSISTENCY_VIOLATION + ']', viewContent.get("IncludeEventTypes"));
        }
    }

    /** */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE, value = "false")
    public void testSensitivePropertiesExcluded() throws Exception {
        checkSensitive(false);
    }

    /** */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE, value = "true")
    public void testSensitivePropertiesIncluded() throws Exception {
        checkSensitive(true);
    }

    /**
     * @param includeSensitive {@code True} if sensitive properties are expected to be exposed.
     */
    private void checkSensitive(boolean includeSensitive) throws Exception {
        String secretKey = "my-secret-key";

        IgniteConfiguration icfg = getConfiguration()
            .setSslContextFactory(sslFactory())
            .setConnectorConfiguration(new TestConnectorConfiguration().setSecretKey(secretKey));

        try (IgniteEx ignite = startGrid(icfg)) {
            Map<String, String> viewContent = new HashMap<>();

            ignite.context().systemView().<ConfigurationView>view(CFG_VIEW)
                .forEach(view -> viewContent.put(view.name(), view.value()));

            assertTrue(viewContent.containsKey("SslContextFactory.KeyStoreFilePath"));
            assertTrue(viewContent.containsKey("ConnectorConfiguration.Port"));

            assertEquals(includeSensitive, viewContent.containsKey("SslContextFactory.KeyStorePassword"));
            assertEquals(includeSensitive, viewContent.containsKey("SslContextFactory.TrustStorePassword"));
            assertEquals(includeSensitive, viewContent.containsKey("ConnectorConfiguration.SecretKey"));
        }
    }

    /** */
    @Test
    public void testCyclicReferences() throws Exception {
        TestPluginConfiguration self = new TestPluginConfiguration();
        self.setRef(self);

        TestPluginConfiguration a = new TestPluginConfiguration();
        TestPluginConfiguration b = new TestPluginConfiguration();
        a.setRef(b);
        b.setRef(a);

        TestPluginConfiguration shared = new TestPluginConfiguration();
        TestPluginConfiguration holder0 = new TestPluginConfiguration();
        TestPluginConfiguration holder1 = new TestPluginConfiguration();
        holder0.setRef(shared);
        holder1.setRef(shared);

        IgniteConfiguration icfg = getConfiguration().setPluginConfigurations(self, a, holder0, holder1, null);

        Map<String, String> viewContent = new HashMap<>();

        for (ConfigurationView view : new IgniteConfigurationIterable(icfg))
            viewContent.put(view.name(), view.value());

        // Self reference.
        assertTrue(viewContent.containsKey("PluginConfigurations[0].Ref"));
        assertFalse(viewContent.containsKey("PluginConfigurations[0].Ref.Ref"));

        // Cyclic reference.
        assertTrue(viewContent.containsKey("PluginConfigurations[1].Ref.Ref"));
        assertFalse(viewContent.containsKey("PluginConfigurations[1].Ref.Ref.Ref"));

        // Shared object without a cycle is exposed for each reference.
        assertTrue(viewContent.containsKey("PluginConfigurations[2].Ref.Ref"));
        assertTrue(viewContent.containsKey("PluginConfigurations[3].Ref.Ref"));

        // Null array element is skipped.
        assertTrue(viewContent.keySet().stream().noneMatch(name -> name.startsWith("PluginConfigurations[4]")));
    }

    /** Tests that an array without elements to iterate is exposed as a plain value. */
    @Test
    public void testArrayWithoutIterableElements() throws Exception {
        TestPluginConfiguration nullsOnly = new TestPluginConfiguration();
        nullsOnly.setRef(new TestPluginConfiguration[] {null, null});

        TestPluginConfiguration cyclicOnly = new TestPluginConfiguration();
        cyclicOnly.setRef(new TestPluginConfiguration[] {cyclicOnly});

        TestPluginConfiguration empty = new TestPluginConfiguration();
        empty.setRef(new TestPluginConfiguration[0]);

        IgniteConfiguration icfg = getConfiguration().setPluginConfigurations(nullsOnly, cyclicOnly, empty);

        Map<String, String> viewContent = new HashMap<>();

        for (ConfigurationView view : new IgniteConfigurationIterable(icfg))
            viewContent.put(view.name(), view.value());

        assertEquals("[null, null]", viewContent.get("PluginConfigurations[0].Ref"));
        assertNotNull(viewContent.get("PluginConfigurations[1].Ref"));
        assertFalse(viewContent.containsKey("PluginConfigurations[1].Ref[0].Ref"));
        assertEquals("[]", viewContent.get("PluginConfigurations[2].Ref"));
    }

    /** Tests that a failed properties collection does not affect the next one. */
    @Test
    public void testFailedCollectionNotCached() throws Exception {
        FailingPluginConfiguration failing = new FailingPluginConfiguration();

        IgniteConfiguration icfg = getConfiguration().setPluginConfigurations(failing);

        IgniteConfigurationIterable iter = new IgniteConfigurationIterable(icfg);

        assertThrows(log, () -> iter.forEach(v -> {}), IgniteException.class, null);

        failing.fail = false;

        Map<String, String> viewContent = new HashMap<>();

        for (ConfigurationView view : iter)
            viewContent.put(view.name(), view.value());

        assertEquals(icfg.getIgniteInstanceName(), viewContent.get("IgniteInstanceName"));
        assertEquals("val", viewContent.get("PluginConfigurations[0].Val"));
    }

    /** Plugin configuration with a reference to another object. */
    public static class TestPluginConfiguration implements PluginConfiguration {
        /** Reference. */
        private Object ref;

        /** @return Reference. */
        public Object getRef() {
            return ref;
        }

        /** @param ref Reference. */
        public void setRef(Object ref) {
            this.ref = ref;
        }
    }

    /** Plugin configuration with a property getter that fails on demand. */
    public static class FailingPluginConfiguration implements PluginConfiguration {
        /** If {@code true}, the getter fails. */
        private volatile boolean fail = true;

        /** @return Value. */
        public String getVal() {
            if (fail)
                throw new IllegalStateException("Test failure.");

            return "val";
        }

        /** @param val Value. */
        public void setVal(String val) {
            // No-op.
        }
    }

    /** Overrides sensitive property getter without {@link SensitiveProperty} annotation. */
    public static class TestConnectorConfiguration extends ConnectorConfiguration {
        /** {@inheritDoc} */
        @Override public String getSecretKey() {
            return super.getSecretKey();
        }
    }
}
