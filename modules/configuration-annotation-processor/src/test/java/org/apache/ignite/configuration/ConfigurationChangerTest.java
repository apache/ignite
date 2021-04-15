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
package org.apache.ignite.configuration;

import java.io.Serializable;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.storage.ConfigurationType;
import org.apache.ignite.configuration.storage.Data;
import org.apache.ignite.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.junit.jupiter.api.Test;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.configuration.AConfiguration.KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test configuration changer.
 */
public class ConfigurationChangerTest {
    /** Annotation used to test failing validation. */
    @Target(FIELD)
    @Retention(RUNTIME)
    @interface MaybeInvalid {
    }

    /** */
    @ConfigurationRoot(rootName = "key", type = ConfigurationType.LOCAL)
    public static class AConfigurationSchema {
        /** */
        @ConfigValue
        @MaybeInvalid
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
        public int intCfg;

        /** */
        @Value
        public String strCfg;
    }

    /** */
    @Config
    public static class CConfigurationSchema {
        /** */
        @Value
        public String strCfg;
    }

    /**
     * Test simple change of configuration.
     */
    @Test
    public void testSimpleConfigurationChange() throws Exception {
        final TestConfigurationStorage storage = new TestConfigurationStorage();

        ANode data = new ANode()
            .initChild(init -> init.initIntCfg(1).initStrCfg("1"))
            .initElements(change -> change.create("a", init -> init.initStrCfg("1")));

        ConfigurationChanger changer = new ConfigurationChanger((oldRoot, newRoot, revision) -> completedFuture(null));
        changer.addRootKey(KEY);
        changer.register(storage);

        changer.change(Collections.singletonMap(KEY, data)).get(1, SECONDS);

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

        ANode data1 = new ANode()
            .initChild(init -> init.initIntCfg(1).initStrCfg("1"))
            .initElements(change -> change.create("a", init -> init.initStrCfg("1")));

        ANode data2 = new ANode()
            .initChild(init -> init.initIntCfg(2).initStrCfg("2"))
            .initElements(change -> change
                .create("a", init -> init.initStrCfg("2"))
                .create("b", init -> init.initStrCfg("2"))
            );

        ConfigurationChanger changer1 = new ConfigurationChanger((oldRoot, newRoot, revision) -> completedFuture(null));
        changer1.addRootKey(KEY);
        changer1.register(storage);

        ConfigurationChanger changer2 = new ConfigurationChanger((oldRoot, newRoot, revision) -> completedFuture(null));
        changer2.addRootKey(KEY);
        changer2.register(storage);

        changer1.change(Collections.singletonMap(KEY, data1)).get(1, SECONDS);
        changer2.change(Collections.singletonMap(KEY, data2)).get(1, SECONDS);

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

        ANode data1 = new ANode()
            .initChild(init -> init.initIntCfg(1).initStrCfg("1"))
            .initElements(change -> change.create("a", init -> init.initStrCfg("1")));

        ANode data2 = new ANode()
            .initChild(init -> init.initIntCfg(2).initStrCfg("2"))
            .initElements(change -> change
                .create("a", init -> init.initStrCfg("2"))
                .create("b", init -> init.initStrCfg("2"))
            );

        ConfigurationChanger changer1 = new ConfigurationChanger((oldRoot, newRoot, revision) -> completedFuture(null));
        changer1.addRootKey(KEY);
        changer1.register(storage);

        ConfigurationChanger changer2 = new ConfigurationChanger((oldRoot, newRoot, revision) -> completedFuture(null));
        changer2.addRootKey(KEY);
        changer2.register(storage);

        changer1.change(Collections.singletonMap(KEY, data1)).get(1, SECONDS);

        changer2.addValidator(MaybeInvalid.class, new Validator<MaybeInvalid, Object>() {
            @Override public void validate(MaybeInvalid annotation, ValidationContext<Object> ctx) {
                ctx.addIssue(new ValidationIssue("foo"));
            }
        });

        assertThrows(ExecutionException.class, () -> changer2.change(Collections.singletonMap(KEY, data2)).get(1, SECONDS));

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

        ANode data = new ANode().initChild(child -> child.initIntCfg(1));

        ConfigurationChanger changer = new ConfigurationChanger((oldRoot, newRoot, revision) -> completedFuture(null));
        changer.addRootKey(KEY);

        storage.fail(true);

        assertThrows(ConfigurationChangeException.class, () -> changer.register(storage));

        storage.fail(false);

        changer.register(storage);

        storage.fail(true);

        assertThrows(ExecutionException.class, () -> changer.change(Collections.singletonMap(KEY, data)).get(1, SECONDS));

        storage.fail(false);

        final Data dataFromStorage = storage.readAll();
        final Map<String, Serializable> dataMap = dataFromStorage.values();

        assertEquals(0, dataMap.size());

        ANode newRoot = (ANode)changer.getRootNode(KEY);
        assertNull(newRoot.child());
    }

    /** */
    @ConfigurationRoot(rootName = "def", type = ConfigurationType.LOCAL)
    public static class DefaultsConfigurationSchema {
        /** */
        @ConfigValue
        private DefaultsChildConfigurationSchema child;

        /** */
        @NamedConfigValue
        private DefaultsChildConfigurationSchema childsList;

        /** */
        @Value(hasDefault = true)
        public String defStr = "foo";
    }

    /** */
    @Config
    public static class DefaultsChildConfigurationSchema {
        /** */
        @Value(hasDefault = true)
        public String defStr = "bar";

        /** */
        @Value(hasDefault = true)
        public String[] arr = {"xyz"};
    }

    @Test
    public void defaultsOnInit() throws Exception {
        var changer = new ConfigurationChanger((oldRoot, newRoot, revision) -> completedFuture(null));

        changer.addRootKey(DefaultsConfiguration.KEY);

        TestConfigurationStorage storage = new TestConfigurationStorage();

        changer.register(storage);

        changer.initialize(storage.type());

        DefaultsNode root = (DefaultsNode)changer.getRootNode(DefaultsConfiguration.KEY);

        assertEquals("foo", root.defStr());
        assertEquals("bar", root.child().defStr());
        assertEquals(List.of("xyz"), Arrays.asList(root.child().arr()));

        // This is not init, move it to another test =(
        changer.change(Map.of(DefaultsConfiguration.KEY, new DefaultsNode().changeChildsList(childs ->
            childs.create("name", child -> {})
        ))).get(1, SECONDS);

        root = (DefaultsNode)changer.getRootNode(DefaultsConfiguration.KEY);

        assertEquals("bar", root.childsList().get("name").defStr());
    }
}
