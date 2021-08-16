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
package org.apache.ignite.internal.configuration;

import java.io.Serializable;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Immutable;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.storage.Data;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.configuration.AConfiguration.KEY;
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
    @ConfigurationRoot(rootName = "key", type = LOCAL)
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
        var storage = new TestConfigurationStorage(LOCAL);

        ConfigurationChanger changer = new TestConfigurationChanger(cgen, List.of(KEY), Map.of(), storage);
        changer.start();

        changer.change(source(KEY, (AChange parent) -> parent
            .changeChild(change -> change.changeIntCfg(1).changeStrCfg("1"))
            .changeElements(change -> change.create("a", element -> element.changeStrCfg("1")))
        )).get(1, SECONDS);

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
        var storage = new TestConfigurationStorage(LOCAL);

        ConfigurationChanger changer1 = new TestConfigurationChanger(cgen, List.of(KEY), Map.of(), storage);
        changer1.start();

        ConfigurationChanger changer2 = new TestConfigurationChanger(cgen, List.of(KEY), Map.of(), storage);
        changer2.start();

        changer1.change(source(KEY, (AChange parent) -> parent
            .changeChild(change -> change.changeIntCfg(1).changeStrCfg("1"))
            .changeElements(change -> change.create("a", element -> element.changeStrCfg("1")))
        )).get(1, SECONDS);

        changer2.change(source(KEY, (AChange parent) -> parent
            .changeChild(change -> change.changeIntCfg(2).changeStrCfg("2"))
            .changeElements(change -> change
                .createOrUpdate("a", element -> element.changeStrCfg("2"))
                .create("b", element -> element.changeStrCfg("2"))
            )
        )).get(1, SECONDS);

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
        var storage = new TestConfigurationStorage(LOCAL);

        ConfigurationChanger changer1 = new TestConfigurationChanger(cgen, List.of(KEY), Map.of(), storage);
        changer1.start();

        Validator<MaybeInvalid, Object> validator = new Validator<>() {
            /** {@inheritDoc} */
            @Override public void validate(MaybeInvalid annotation, ValidationContext<Object> ctx) {
                ctx.addIssue(new ValidationIssue("foo"));
            }
        };

        ConfigurationChanger changer2 = new TestConfigurationChanger(
            cgen,
            List.of(KEY),
            Map.of(MaybeInvalid.class, Set.of(validator)),
            storage
        );

        changer2.start();

        changer1.change(source(KEY, (AChange parent) -> parent
            .changeChild(change -> change.changeIntCfg(1).changeStrCfg("1"))
            .changeElements(change -> change.create("a", element -> element.changeStrCfg("1")))
        )).get(1, SECONDS);

        assertThrows(ExecutionException.class, () -> changer2.change(source(KEY, (AChange parent) -> parent
            .changeChild(change -> change.changeIntCfg(2).changeStrCfg("2"))
            .changeElements(change -> change
                .create("a", element -> element.changeStrCfg("2"))
                .create("b", element -> element.changeStrCfg("2"))
            )
        )).get(1, SECONDS));

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
        var storage = new TestConfigurationStorage(LOCAL);

        ConfigurationChanger changer = new TestConfigurationChanger(cgen, List.of(KEY), Map.of(), storage);

        storage.fail(true);

        assertThrows(ConfigurationChangeException.class, changer::start);

        storage.fail(false);

        changer.start();

        storage.fail(true);

        assertThrows(ExecutionException.class, () -> changer.change(source(KEY, (AChange parent) -> parent
            .changeChild(child -> child.changeIntCfg(1))
        )).get(1, SECONDS));

        storage.fail(false);

        final Data dataFromStorage = storage.readAll();
        final Map<String, Serializable> dataMap = dataFromStorage.values();

        assertEquals(0, dataMap.size());

        AView newRoot = (AView)changer.getRootNode(KEY);
        assertNull(newRoot.child());
    }

    /** */
    @ConfigurationRoot(rootName = "def", type = LOCAL)
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
        var storage = new TestConfigurationStorage(LOCAL);

        var changer = new TestConfigurationChanger(cgen, List.of(DefaultsConfiguration.KEY), Map.of(), storage);

        changer.start();

        changer.initializeDefaults();

        DefaultsView root = (DefaultsView)changer.getRootNode(DefaultsConfiguration.KEY);

        assertEquals("foo", root.defStr());
        assertEquals("bar", root.child().defStr());
        assertEquals(List.of("xyz"), Arrays.asList(root.child().arr()));

        changer.change(source(DefaultsConfiguration.KEY, (DefaultsChange def) -> def
            .changeChildsList(childs ->
                childs.create("name", child -> {})
            )
        )).get(1, SECONDS);

        root = (DefaultsView)changer.getRootNode(DefaultsConfiguration.KEY);

        assertEquals("bar", root.childsList().get("name").defStr());
    }

    private static <Change> ConfigurationSource source(RootKey<?, ? super Change> rootKey, Consumer<Change> changer) {
        return new ConfigurationSource() {
            @Override public void descend(ConstructableTreeNode node) {
                ConfigurationSource changerSrc = new ConfigurationSource() {
                    @Override public void descend(ConstructableTreeNode node) {
                        changer.accept((Change)node);
                    }
                };

                node.construct(rootKey.key(), changerSrc);
            }
        };
    }
}
