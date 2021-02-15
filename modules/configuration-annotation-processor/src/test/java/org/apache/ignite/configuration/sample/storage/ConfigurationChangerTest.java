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
package org.apache.ignite.configuration.sample.storage;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.ConfigurationChanger;
import org.apache.ignite.configuration.Configurator;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.sample.storage.impl.ANode;
import org.apache.ignite.configuration.storage.Data;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test configuration changer.
 */
public class ConfigurationChangerTest {
    /** Root configuration key. */
    private static final RootKey<?> KEY = () -> "key";

    /** */
    @Config
    public static class AConfigurationSchema {
        /** */
        @ConfigValue
        private BConfigurationSchema child;

        /** */
        @NamedConfigValue
        private CConfigurationSchema elements;
    }

    /** */
    @Config
    public static class BConfigurationSchema {
        /** */
        @Value(immutable = true)
        private int intCfg;

        /** */
        @Value
        private String strCfg;
    }

    /** */
    @Config
    public static class CConfigurationSchema {
        /** */
        @Value
        private String strCfg;
    }

    /**
     * Test simple change of configuration.
     */
    @Test
    public void testSimpleConfigurationChange() throws Exception {
        final TestConfigurationStorage storage = new TestConfigurationStorage();

        final ConfiguratorController configuratorController = new ConfiguratorController();
        final Configurator<?> configurator = configuratorController.configurator();

        ANode data = new ANode()
            .initChild(init -> init.initIntCfg(1).initStrCfg("1"))
            .initElements(change -> change.create("a", init -> init.initStrCfg("1")));

        final ConfigurationChanger changer = new ConfigurationChanger(storage);
        changer.init();

        changer.registerConfiguration(KEY, configurator);

        changer.change(Collections.singletonMap(KEY, data)).get();

        final Data dataFromStorage = storage.readAll();
        final Map<String, Serializable> dataMap = dataFromStorage.values();

        assertEquals(3, dataMap.size());
        assertThat(dataMap, hasEntry("key.child.intCfg", 1));
        assertThat(dataMap, hasEntry("key.child.strCfg", "1"));
        assertThat(dataMap, hasEntry("key.elements.a.strCfg", "1"));
    }

    /**
     * Test subsequent change of configuration via different changers.
     */
    @Test
    public void testModifiedFromAnotherStorage() throws Exception {
        final TestConfigurationStorage storage = new TestConfigurationStorage();

        final ConfiguratorController configuratorController = new ConfiguratorController();
        final Configurator<?> configurator = configuratorController.configurator();

        ANode data1 = new ANode()
            .initChild(init -> init.initIntCfg(1).initStrCfg("1"))
            .initElements(change -> change.create("a", init -> init.initStrCfg("1")));

        ANode data2 = new ANode()
            .initChild(init -> init.initIntCfg(2).initStrCfg("2"))
            .initElements(change -> change
                .create("a", init -> init.initStrCfg("2"))
                .create("b", init -> init.initStrCfg("2"))
            );

        final ConfigurationChanger changer1 = new ConfigurationChanger(storage);
        changer1.init();

        final ConfigurationChanger changer2 = new ConfigurationChanger(storage);
        changer2.init();

        changer1.registerConfiguration(KEY, configurator);
        changer2.registerConfiguration(KEY, configurator);

        changer1.change(Collections.singletonMap(KEY, data1)).get();
        changer2.change(Collections.singletonMap(KEY, data2)).get();

        final Data dataFromStorage = storage.readAll();
        final Map<String, Serializable> dataMap = dataFromStorage.values();

        assertEquals(4, dataMap.size());
        assertThat(dataMap, hasEntry("key.child.intCfg", 2));
        assertThat(dataMap, hasEntry("key.child.strCfg", "2"));
        assertThat(dataMap, hasEntry("key.elements.a.strCfg", "2"));
        assertThat(dataMap, hasEntry("key.elements.b.strCfg", "2"));
    }

    /**
     * Test that subsequent change of configuration is failed if changes are incompatible.
     */
    @Test
    public void testModifiedFromAnotherStorageWithIncompatibleChanges() throws Exception {
        final TestConfigurationStorage storage = new TestConfigurationStorage();

        final ConfiguratorController configuratorController = new ConfiguratorController();
        final Configurator<?> configurator = configuratorController.configurator();

        ANode data1 = new ANode()
            .initChild(init -> init.initIntCfg(1).initStrCfg("1"))
            .initElements(change -> change.create("a", init -> init.initStrCfg("1")));

        ANode data2 = new ANode()
            .initChild(init -> init.initIntCfg(2).initStrCfg("2"))
            .initElements(change -> change
                .create("a", init -> init.initStrCfg("2"))
                .create("b", init -> init.initStrCfg("2"))
            );

        final ConfigurationChanger changer1 = new ConfigurationChanger(storage);
        changer1.init();

        final ConfigurationChanger changer2 = new ConfigurationChanger(storage);
        changer2.init();

        changer1.registerConfiguration(KEY, configurator);
        changer2.registerConfiguration(KEY, configurator);

        changer1.change(Collections.singletonMap(KEY, data1)).get();

        configuratorController.hasIssues(true);

        assertThrows(ExecutionException.class, () -> changer2.change(Collections.singletonMap(KEY, data2)).get());

        final Data dataFromStorage = storage.readAll();
        final Map<String, Serializable> dataMap = dataFromStorage.values();

        assertEquals(3, dataMap.size());
        assertThat(dataMap, hasEntry("key.child.intCfg", 1));
        assertThat(dataMap, hasEntry("key.child.strCfg", "1"));
        assertThat(dataMap, hasEntry("key.elements.a.strCfg", "1"));
    }

    /**
     * Test that init and change fail with right exception if storage is inaccessible.
     */
    @Test
    public void testFailedToWrite() {
        final TestConfigurationStorage storage = new TestConfigurationStorage();

        final ConfiguratorController configuratorController = new ConfiguratorController();
        final Configurator<?> configurator = configuratorController.configurator();

        ANode data = new ANode();

        final ConfigurationChanger changer = new ConfigurationChanger(storage);

        storage.fail(true);

        assertThrows(ConfigurationChangeException.class, changer::init);

        storage.fail(false);

        changer.init();

        changer.registerConfiguration(KEY, configurator);

        storage.fail(true);

        assertThrows(ExecutionException.class, () -> changer.change(Collections.singletonMap(KEY, data)).get());

        storage.fail(false);

        final Data dataFromStorage = storage.readAll();
        final Map<String, Serializable> dataMap = dataFromStorage.values();

        assertEquals(0, dataMap.size());
    }

    /**
     * Wrapper for Configurator mock to control validation.
     */
    private static class ConfiguratorController {
        /** Configurator. */
        final Configurator<?> configurator;

        /** Whether validate method should return issues. */
        private boolean hasIssues;

        /** Constructor. */
        private ConfiguratorController() {
            this(false);
        }

        /** Constructor. */
        private ConfiguratorController(boolean hasIssues) {
            this.hasIssues = hasIssues;

            configurator = Mockito.mock(Configurator.class);

            Mockito.when(configurator.validateChanges(Mockito.any())).then(mock -> {
                if (this.hasIssues)
                    return Collections.singletonList(new ValidationIssue());

                return Collections.emptyList();
            });
        }

        /**
         * Set has issues flag.
         * @param hasIssues Has issues flag.
         */
        public void hasIssues(boolean hasIssues) {
            this.hasIssues = hasIssues;
        }

        /**
         * Get configurator.
         * @return Configurator.
         */
        public Configurator<?> configurator() {
            return configurator;
        }
    }
}
