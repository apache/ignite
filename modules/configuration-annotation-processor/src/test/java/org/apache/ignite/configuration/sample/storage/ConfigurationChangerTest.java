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
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.sample.storage.impl.ANode;
import org.apache.ignite.configuration.storage.Data;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.apache.ignite.configuration.sample.storage.AConfiguration.KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test configuration changer.
 */
public class ConfigurationChangerTest {
    /** */
    @ConfigurationRoot(rootName = "key", storage = TestConfigurationStorage.class)
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
        changer.init(KEY);

        changer.registerConfiguration(KEY, configurator);

        changer.change(Collections.singletonMap(KEY, data)).get();

        ANode newRoot = (ANode)changer.getRootNode(KEY);

        assertEquals(1, newRoot.child().intCfg());
        assertEquals("1", newRoot.child().strCfg());
        assertEquals("1", newRoot.elements().get("a").strCfg());
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
        changer1.init(KEY);

        final ConfigurationChanger changer2 = new ConfigurationChanger(storage);
        changer2.init(KEY);

        changer1.registerConfiguration(KEY, configurator);
        changer2.registerConfiguration(KEY, configurator);

        changer1.change(Collections.singletonMap(KEY, data1)).get();
        changer2.change(Collections.singletonMap(KEY, data2)).get();

        ANode newRoot1 = (ANode)changer1.getRootNode(KEY);

        assertEquals(2, newRoot1.child().intCfg());
        assertEquals("2", newRoot1.child().strCfg());
        assertEquals("2", newRoot1.elements().get("a").strCfg());
        assertEquals("2", newRoot1.elements().get("b").strCfg());

        ANode newRoot2 = (ANode)changer2.getRootNode(KEY);

        assertEquals(2, newRoot2.child().intCfg());
        assertEquals("2", newRoot2.child().strCfg());
        assertEquals("2", newRoot2.elements().get("a").strCfg());
        assertEquals("2", newRoot2.elements().get("b").strCfg());
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
        changer1.init(KEY);

        final ConfigurationChanger changer2 = new ConfigurationChanger(storage);
        changer2.init(KEY);

        changer1.registerConfiguration(KEY, configurator);
        changer2.registerConfiguration(KEY, configurator);

        changer1.change(Collections.singletonMap(KEY, data1)).get();

        configuratorController.hasIssues(true);

        assertThrows(ExecutionException.class, () -> changer2.change(Collections.singletonMap(KEY, data2)).get());

        ANode newRoot = (ANode)changer2.getRootNode(KEY);

        assertEquals(1, newRoot.child().intCfg());
        assertEquals("1", newRoot.child().strCfg());
        assertEquals("1", newRoot.elements().get("a").strCfg());
    }

    /**
     * Test that init and change fail with right exception if storage is inaccessible.
     */
    @Test
    public void testFailedToWrite() {
        final TestConfigurationStorage storage = new TestConfigurationStorage();

        final ConfiguratorController configuratorController = new ConfiguratorController();
        final Configurator<?> configurator = configuratorController.configurator();

        ANode data = new ANode().initChild(child -> child.initIntCfg(1));

        final ConfigurationChanger changer = new ConfigurationChanger(storage);

        storage.fail(true);

        assertThrows(ConfigurationChangeException.class, () -> changer.init(KEY));

        storage.fail(false);

        changer.init(KEY);

        changer.registerConfiguration(KEY, configurator);

        storage.fail(true);

        assertThrows(ExecutionException.class, () -> changer.change(Collections.singletonMap(KEY, data)).get());

        storage.fail(false);

        final Data dataFromStorage = storage.readAll();
        final Map<String, Serializable> dataMap = dataFromStorage.values();

        assertEquals(0, dataMap.size());

        ANode newRoot = (ANode)changer.getRootNode(KEY);
        assertNull(newRoot.child());
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
