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
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.NamedListView;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

    /** */
    private static ConfigurationAsmGenerator cgen = new ConfigurationAsmGenerator();

    /** Test storage. */
    private final TestConfigurationStorage storage = new TestConfigurationStorage(LOCAL);

    /** */
    @AfterAll
    public static void afterAll() {
        cgen = null;
    }

    /**
     * Test simple change of configuration.
     */
    @Test
    public void testSimpleConfigurationChange() throws Exception {
        ConfigurationChanger changer = createChanger(KEY);
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
        ConfigurationChanger changer1 = createChanger(KEY);
        changer1.start();

        ConfigurationChanger changer2 = createChanger(KEY);
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
        ConfigurationChanger changer1 = createChanger(KEY);
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
            storage,
            List.of()
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
        ConfigurationChanger changer = createChanger(KEY);

        storage.fail(true);

        assertThrows(ConfigurationChangeException.class, changer::start);

        storage.fail(false);

        changer.start();

        storage.fail(true);

        assertThrows(ExecutionException.class, () -> changer.change(source(KEY, (AChange parent) -> parent
            .changeChild(child -> child.changeIntCfg(1).changeStrCfg("1"))
        )).get(1, SECONDS));

        storage.fail(false);

        Data dataFromStorage = storage.readAll();
        Map<String, ? extends Serializable> dataMap = dataFromStorage.values();

        assertEquals(0, dataMap.size());

        AView newRoot = (AView)changer.getRootNode(KEY);
        assertNotNull(newRoot.child());
        assertNull(newRoot.child().strCfg());
    }

    /** */
    @ConfigurationRoot(rootName = "def", type = LOCAL)
    public static class DefaultsConfigurationSchema {
        /** */
        @ConfigValue
        public DefaultsChildConfigurationSchema child;

        /** */
        @NamedConfigValue
        public DefaultsChildConfigurationSchema childrenList;

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
        var changer = createChanger(DefaultsConfiguration.KEY);

        changer.start();

        changer.initializeDefaults();

        DefaultsView root = (DefaultsView)changer.getRootNode(DefaultsConfiguration.KEY);

        assertEquals("foo", root.defStr());
        assertEquals("bar", root.child().defStr());
        assertEquals(List.of("xyz"), Arrays.asList(root.child().arr()));

        changer.change(source(DefaultsConfiguration.KEY, (DefaultsChange def) -> def
            .changeChildrenList(children ->
                children.create("name", child -> {})
            )
        )).get(1, SECONDS);

        root = (DefaultsView)changer.getRootNode(DefaultsConfiguration.KEY);

        assertEquals("bar", root.childrenList().get("name").defStr());
    }

    /**
     * Tests the {@link DynamicConfigurationChanger#getLatest} method by retrieving different configuration
     * values.
     */
    @Test
    public void testGetLatest() throws Exception {
        ConfigurationChanger changer = createChanger(DefaultsConfiguration.KEY);

        changer.start();

        changer.initializeDefaults();

        ConfigurationSource source = source(
            DefaultsConfiguration.KEY,
            (DefaultsChange change) -> change.changeChildrenList(children -> children.create("name", child -> {}))
        );

        changer.change(source).get(1, SECONDS);

        DefaultsView configurationView = changer.getLatest(List.of("def"));

        assertEquals("foo", configurationView.defStr());
        assertEquals("bar", configurationView.child().defStr());
        assertArrayEquals(new String[] {"xyz"}, configurationView.child().arr());
        assertEquals("bar", configurationView.childrenList().get("name").defStr());
    }

    /**
     * Tests the {@link DynamicConfigurationChanger#getLatest} method by retrieving different nested configuration
     * values.
     */
    @Test
    public void testGetLatestNested() {
        ConfigurationChanger changer = createChanger(DefaultsConfiguration.KEY);

        changer.start();

        changer.initializeDefaults();

        DefaultsChildView childView = changer.getLatest(List.of("def", "child"));

        assertEquals("bar", childView.defStr());
        assertArrayEquals(new String[] {"xyz"}, childView.arr());

        String childStrValueView = changer.getLatest(List.of("def", "child", "defStr"));

        assertEquals("bar", childStrValueView);

        String[] childArrView = changer.getLatest(List.of("def", "child", "arr"));

        assertArrayEquals(new String[] {"xyz"}, childArrView);
    }

    /**
     * Tests the {@link DynamicConfigurationChanger#getLatest} method by retrieving different Named List configuration
     * values.
     */
    @Test
    public void testGetLatestNamedList() throws Exception {
        ConfigurationChanger changer = createChanger(DefaultsConfiguration.KEY);

        changer.start();

        changer.initializeDefaults();

        ConfigurationSource source = source(
            DefaultsConfiguration.KEY,
            (DefaultsChange change) -> change.changeChildrenList(children -> {
                children.create("name1", child -> {});
                children.create("name2", child -> {});
            })
        );

        changer.change(source).get(1, SECONDS);

        for (String name : List.of("name1", "name2")) {
            NamedListView<DefaultsChildView> childrenListView = changer.getLatest(List.of("def", "childrenList"));

            DefaultsChildView childrenListElementView = childrenListView.get(name);

            assertEquals("bar", childrenListElementView.defStr());
            assertArrayEquals(new String[] {"xyz"}, childrenListElementView.arr());

            childrenListElementView = changer.getLatest(List.of("def", "childrenList", name));

            assertEquals("bar", childrenListElementView.defStr());
            assertArrayEquals(new String[] {"xyz"}, childrenListElementView.arr());

            String childrenListStrValueView = changer.getLatest(List.of("def", "childrenList", name, "defStr"));

            assertEquals("bar", childrenListStrValueView);

            String[] childrenListArrView = changer.getLatest(List.of("def", "childrenList", name, "arr"));

            assertArrayEquals(new String[] {"xyz"}, childrenListArrView);
        }
    }

    /**
     * Tests the {@link DynamicConfigurationChanger#getLatest} method by trying to find non-existend values on
     * different configuration levels.
     */
    @Test
    public void testGetLatestMissingKey() throws Exception {
        ConfigurationChanger changer = createChanger(DefaultsConfiguration.KEY);

        changer.start();

        changer.initializeDefaults();

        ConfigurationSource source = source(
            DefaultsConfiguration.KEY,
            (DefaultsChange change) -> change.changeChildrenList(children -> children.create("name", child -> {}))
        );

        changer.change(source).get(1, SECONDS);

        NoSuchElementException e = assertThrows(NoSuchElementException.class, () -> changer.getLatest(List.of("foo")));

        assertThat(e.getMessage(), containsString("foo"));

        e = assertThrows(NoSuchElementException.class, () -> changer.getLatest(List.of("def", "foo")));

        assertThat(e.getMessage(), containsString("foo"));

        e = assertThrows(NoSuchElementException.class, () -> changer.getLatest(List.of("def", "defStr", "foo")));

        assertThat(e.getMessage(), containsString("def.defStr.foo"));

        e = assertThrows(NoSuchElementException.class, () -> changer.getLatest(List.of("def", "child", "foo")));

        assertThat(e.getMessage(), containsString("foo"));

        e = assertThrows(
            NoSuchElementException.class,
            () -> changer.getLatest(List.of("def", "child", "defStr", "foo"))
        );

        assertThat(e.getMessage(), containsString("def.child.defStr.foo"));

        e = assertThrows(NoSuchElementException.class, () -> changer.getLatest(List.of("def", "childrenList", "foo")));

        assertThat(e.getMessage(), containsString("def.childrenList.foo"));

        e = assertThrows(
            NoSuchElementException.class,
            () -> changer.getLatest(List.of("def", "childrenList", "name", "foo"))
        );

        assertThat(e.getMessage(), containsString("def.childrenList.name.foo"));

        e = assertThrows(
            NoSuchElementException.class,
            () -> changer.getLatest(List.of("def", "childrenList", "name", "defStr", "foo"))
        );

        assertThat(e.getMessage(), containsString("def.childrenList.name.defStr"));
    }

    /** */
    private static <Change> ConfigurationSource source(RootKey<?, ? super Change> rootKey, Consumer<Change> changer) {
        return new ConfigurationSource() {
            @Override public void descend(ConstructableTreeNode node) {
                ConfigurationSource changerSrc = new ConfigurationSource() {
                    @Override public void descend(ConstructableTreeNode node) {
                        changer.accept((Change)node);
                    }
                };

                node.construct(rootKey.key(), changerSrc, true);
            }
        };
    }

    /** */
    private ConfigurationChanger createChanger(RootKey<?, ?> rootKey) {
        return new TestConfigurationChanger(cgen, List.of(rootKey), Map.of(), storage, List.of());
    }
}
