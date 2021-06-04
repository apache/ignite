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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.internal.asm.ConfigurationAsmGenerator;
import org.apache.ignite.configuration.storage.ConfigurationType;
import org.apache.ignite.configuration.storage.Data;
import org.apache.ignite.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.configuration.tree.InnerNode;
import org.apache.ignite.configuration.tree.TraversableTreeNode;
import org.apache.ignite.configuration.validation.Immutable;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.configuration.AConfiguration.KEY;
import static org.apache.ignite.configuration.internal.util.ConfigurationUtil.superRootPatcher;
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
        public BConfigurationSchema child;

        /** */
        @NamedConfigValue
        public CConfigurationSchema elements;
    }

    /** */
    @Config
    public static class BConfigurationSchema {
        /** */
        @Value
        @Immutable
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

    private static ConfigurationAsmGenerator cgen = new ConfigurationAsmGenerator();

    @AfterAll
    public static void afterAll() {
        cgen = null;
    }

    /**
     * Test simple change of configuration.
     */
    @Test
    public void testSimpleConfigurationChange() throws Exception {
        final TestConfigurationStorage storage = new TestConfigurationStorage();

        ConfigurationChanger changer = new TestConfigurationChanger();
        changer.addRootKey(KEY);
        changer.register(storage);

        AChange data = ((AChange)changer.createRootNode(KEY))
            .changeChild(change -> change.changeIntCfg(1).changeStrCfg("1"))
            .changeElements(change -> change.create("a", element -> element.changeStrCfg("1")));

        changer.change(superRootPatcher(KEY, (TraversableTreeNode)data), null).get(1, SECONDS);

        AView newRoot = (AView)changer.getRootNode(KEY);

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

        ConfigurationChanger changer1 = new TestConfigurationChanger();
        changer1.addRootKey(KEY);
        changer1.register(storage);

        ConfigurationChanger changer2 = new TestConfigurationChanger();
        changer2.addRootKey(KEY);
        changer2.register(storage);

        AChange data1 = ((AChange)changer1.createRootNode(KEY))
            .changeChild(change -> change.changeIntCfg(1).changeStrCfg("1"))
            .changeElements(change -> change.create("a", element -> element.changeStrCfg("1")));

        changer1.change(superRootPatcher(KEY, (TraversableTreeNode)data1), null).get(1, SECONDS);

        AChange data2 = ((AChange)changer2.createRootNode(KEY))
            .changeChild(change -> change.changeIntCfg(2).changeStrCfg("2"))
            .changeElements(change -> change
                .create("a", element -> element.changeStrCfg("2"))
                .create("b", element -> element.changeStrCfg("2"))
            );

        changer2.change(superRootPatcher(KEY, (TraversableTreeNode)data2), null).get(1, SECONDS);

        AView newRoot1 = (AView)changer1.getRootNode(KEY);

        assertEquals(2, newRoot1.child().intCfg());
        assertEquals("2", newRoot1.child().strCfg());
        assertEquals("2", newRoot1.elements().get("a").strCfg());
        assertEquals("2", newRoot1.elements().get("b").strCfg());

        AView newRoot2 = (AView)changer2.getRootNode(KEY);

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

        ConfigurationChanger changer1 = new TestConfigurationChanger();
        changer1.addRootKey(KEY);
        changer1.register(storage);

        ConfigurationChanger changer2 = new TestConfigurationChanger();
        changer2.addRootKey(KEY);
        changer2.register(storage);

        AChange data1 = ((AChange)changer1.createRootNode(KEY))
            .changeChild(change -> change.changeIntCfg(1).changeStrCfg("1"))
            .changeElements(change -> change.create("a", element -> element.changeStrCfg("1")));

        changer1.change(superRootPatcher(KEY, (TraversableTreeNode)data1), null).get(1, SECONDS);

        changer2.addValidator(MaybeInvalid.class, new Validator<MaybeInvalid, Object>() {
            @Override public void validate(MaybeInvalid annotation, ValidationContext<Object> ctx) {
                ctx.addIssue(new ValidationIssue("foo"));
            }
        });

        AChange data2 = ((AChange)changer2.createRootNode(KEY))
            .changeChild(change -> change.changeIntCfg(2).changeStrCfg("2"))
            .changeElements(change -> change
                .create("a", element -> element.changeStrCfg("2"))
                .create("b", element -> element.changeStrCfg("2"))
            );

        assertThrows(ExecutionException.class, () -> changer2.change(superRootPatcher(KEY, (TraversableTreeNode)data2), null).get(1, SECONDS));

        AView newRoot = (AView)changer2.getRootNode(KEY);

        assertEquals(1, newRoot.child().intCfg());
        assertEquals("1", newRoot.child().strCfg());
        assertEquals("1", newRoot.elements().get("a").strCfg());
    }

    /**
     * Test that change fails with right exception if storage is inaccessible.
     */
    @Test
    public void testFailedToWrite() {
        final TestConfigurationStorage storage = new TestConfigurationStorage();

        ConfigurationChanger changer = new TestConfigurationChanger();
        changer.addRootKey(KEY);

        storage.fail(true);

        assertThrows(ConfigurationChangeException.class, () -> changer.register(storage));

        storage.fail(false);

        changer.register(storage);

        storage.fail(true);

        AChange data = ((AChange)changer.createRootNode(KEY)).changeChild(child -> child.changeIntCfg(1));

        assertThrows(ExecutionException.class, () -> changer.change(superRootPatcher(KEY, (TraversableTreeNode)data), null).get(1, SECONDS));

        storage.fail(false);

        final Data dataFromStorage = storage.readAll();
        final Map<String, Serializable> dataMap = dataFromStorage.values();

        assertEquals(0, dataMap.size());

        AView newRoot = (AView)changer.getRootNode(KEY);
        assertNull(newRoot.child());
    }

    /** */
    @ConfigurationRoot(rootName = "def", type = ConfigurationType.LOCAL)
    public static class DefaultsConfigurationSchema {
        /** */
        @ConfigValue
        public DefaultsChildConfigurationSchema child;

        /** */
        @NamedConfigValue
        public DefaultsChildConfigurationSchema childsList;

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
        var changer = new TestConfigurationChanger();

        changer.addRootKey(DefaultsConfiguration.KEY);

        TestConfigurationStorage storage = new TestConfigurationStorage();

        changer.register(storage);

        changer.initialize(storage.type());

        DefaultsView root = (DefaultsView)changer.getRootNode(DefaultsConfiguration.KEY);

        assertEquals("foo", root.defStr());
        assertEquals("bar", root.child().defStr());
        assertEquals(List.of("xyz"), Arrays.asList(root.child().arr()));

        DefaultsChange change = ((DefaultsChange)changer.createRootNode(DefaultsConfiguration.KEY)).changeChildsList(childs ->
            childs.create("name", child -> {})
        );

        changer.change(superRootPatcher(DefaultsConfiguration.KEY, (TraversableTreeNode)change), null).get(1, SECONDS);

        root = (DefaultsView)changer.getRootNode(DefaultsConfiguration.KEY);

        assertEquals("bar", root.childsList().get("name").defStr());
    }

    private static class TestConfigurationChanger extends ConfigurationChanger {
        TestConfigurationChanger() {
            super((oldRoot, newRoot, revision) -> completedFuture(null));
        }

        /** {@inheritDoc} */
        @Override public void addRootKey(RootKey<?, ?> rootKey) {
            super.addRootKey(rootKey);

            cgen.compileRootSchema(rootKey.schemaClass());
        }

        /** {@inheritDoc} */
        @Override public InnerNode createRootNode(RootKey<?, ?> rootKey) {
            return cgen.instantiateNode(rootKey.schemaClass());
        }
    }
}
