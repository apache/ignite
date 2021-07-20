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

package org.apache.ignite.internal.configuration.util;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.SuperRoot;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.TraversableTreeNode;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.internal.configuration.tree.NamedListNode.ORDER_IDX;
import static org.apache.ignite.internal.configuration.util.ConfigurationFlattener.createFlattenedUpdatesMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** */
public class ConfigurationUtilTest {
    private static ConfigurationAsmGenerator cgen;

    @BeforeAll
    public static void beforeAll() {
        cgen = new ConfigurationAsmGenerator();

        cgen.compileRootSchema(ParentConfigurationSchema.class);
    }

    @AfterAll
    public static void afterAll() {
        cgen = null;
    }

    public static <P extends InnerNode & ParentChange> P newParentInstance() {
        return (P)cgen.instantiateNode(ParentConfigurationSchema.class);
    }

    /** */
    @Test
    public void escape() {
        assertEquals("foo", ConfigurationUtil.escape("foo"));

        assertEquals("foo\\.bar", ConfigurationUtil.escape("foo.bar"));

        assertEquals("foo\\\\bar", ConfigurationUtil.escape("foo\\bar"));

        assertEquals("\\\\a\\.b\\\\c\\.", ConfigurationUtil.escape("\\a.b\\c."));
    }

    /** */
    @Test
    public void unescape() {
        assertEquals("foo", ConfigurationUtil.unescape("foo"));

        assertEquals("foo.bar", ConfigurationUtil.unescape("foo\\.bar"));

        assertEquals("foo\\bar", ConfigurationUtil.unescape("foo\\\\bar"));

        assertEquals("\\a.b\\c.", ConfigurationUtil.unescape("\\\\a\\.b\\\\c\\."));
    }

    /** */
    @Test
    public void split() {
        assertEquals(List.of("a", "b.b", "c\\c", ""), ConfigurationUtil.split("a.b\\.b.c\\\\c."));
    }

    /** */
    @Test
    public void join() {
        assertEquals("a.b\\.b.c\\\\c", ConfigurationUtil.join(List.of("a", "b.b", "c\\c")));
    }

    /** */
    @ConfigurationRoot(rootName = "root", type = ConfigurationType.LOCAL)
    public static class ParentConfigurationSchema {
        /** */
        @NamedConfigValue
        public NamedElementConfigurationSchema elements;
    }

    /** */
    @Config
    public static class NamedElementConfigurationSchema {
        /** */
        @ConfigValue
        public ChildConfigurationSchema child;
    }

    /** */
    @Config
    public static class ChildConfigurationSchema {
        /** */
        @Value
        public String str;
    }

    /**
     * Tests that {@link ConfigurationUtil#find(List, TraversableTreeNode)} finds proper node when provided with correct
     * path.
     */
    @Test
    public void findSuccessfully() {
        var parent = newParentInstance();

        parent.changeElements(elements ->
            elements.createOrUpdate("name", element ->
                element.changeChild(child ->
                    child.changeStr("value")
                )
            )
        );

        assertSame(
            parent,
            ConfigurationUtil.find(List.of(), parent)
        );

        assertSame(
            parent.elements(),
            ConfigurationUtil.find(List.of("elements"), parent)
        );

        assertSame(
            parent.elements().get("name"),
            ConfigurationUtil.find(List.of("elements", "name"), parent)
        );

        assertSame(
            parent.elements().get("name").child(),
            ConfigurationUtil.find(List.of("elements", "name", "child"), parent)
        );

        assertSame(
            parent.elements().get("name").child().str(),
            ConfigurationUtil.find(List.of("elements", "name", "child", "str"), parent)
        );
    }

    /**
     * Tests that {@link ConfigurationUtil#find(List, TraversableTreeNode)} returns null when path points to nonexistent
     * named list element.
     */
    @Test
    public void findNulls() {
        var parent = newParentInstance();

        assertNull(ConfigurationUtil.find(List.of("elements", "name"), parent));

        parent.changeElements(elements -> elements.createOrUpdate("name", element -> {}));

        assertNull(ConfigurationUtil.find(List.of("elements", "name", "child"), parent));

        ((NamedElementChange)parent.elements().get("name")).changeChild(child -> {});

        assertNull(ConfigurationUtil.find(List.of("elements", "name", "child", "str"), parent));
    }

    /**
     * Tests that {@link ConfigurationUtil#find(List, TraversableTreeNode)} throws {@link KeyNotFoundException} when
     * provided with a wrong path.
     */
    @Test
    public void findUnsuccessfully() {
        var parent = newParentInstance();

        assertThrows(
            KeyNotFoundException.class,
            () -> ConfigurationUtil.find(List.of("elements", "name", "child"), parent)
        );

        parent.changeElements(elements -> elements.createOrUpdate("name", element -> {}));

        assertThrows(
            KeyNotFoundException.class,
            () -> ConfigurationUtil.find(List.of("elements", "name", "child", "str"), parent)
        );

        ((NamedElementChange)parent.elements().get("name")).changeChild(child -> child.changeStr("value"));

        assertThrows(
            KeyNotFoundException.class,
            () -> ConfigurationUtil.find(List.of("elements", "name", "child", "str", "foo"), parent)
        );
    }

    /**
     * Tests convertion of flat map to a prefix map.
     */
    @Test
    public void toPrefixMap() {
        assertEquals(
            Map.of("foo", 42),
            ConfigurationUtil.toPrefixMap(Map.of("foo", 42))
        );

        assertEquals(
            Map.of("foo.bar", 42),
            ConfigurationUtil.toPrefixMap(Map.of("foo\\.bar", 42))
        );

        assertEquals(
            Map.of("foo", Map.of("bar1", 10, "bar2", 20)),
            ConfigurationUtil.toPrefixMap(Map.of("foo.bar1", 10, "foo.bar2", 20))
        );

        assertEquals(
            Map.of("root1", Map.of("leaf1", 10), "root2", Map.of("leaf2", 20)),
            ConfigurationUtil.toPrefixMap(Map.of("root1.leaf1", 10, "root2.leaf2", 20))
        );
    }

    /**
     * Tests that patching of configuration node with a prefix map works fine when prefix map is valid.
     */
    @Test
    public void fillFromPrefixMapSuccessfully() {
        var parentNode = newParentInstance();

        ConfigurationUtil.fillFromPrefixMap(parentNode, Map.of(
            "elements", Map.of(
                "name2", Map.of(
                    "child", Map.of("str", "value2"),
                    ORDER_IDX, 1
                ),
                "name1", Map.of(
                    "child", Map.of("str", "value1"),
                    ORDER_IDX, 0
                )
            )
        ));

        assertEquals("value1", parentNode.elements().get("name1").child().str());
        assertEquals("value2", parentNode.elements().get("name2").child().str());
    }

    /**
     * Tests that patching of configuration node with a prefix map works fine when prefix map is valid.
     */
    @Test
    public void fillFromPrefixMapSuccessfullyWithRemove() {
        var parentNode = newParentInstance();

        parentNode.changeElements(elements ->
            elements.createOrUpdate("name", element ->
                element.changeChild(child -> {})
            )
        );

        ConfigurationUtil.fillFromPrefixMap(parentNode, Map.of(
            "elements", singletonMap("name", null)
        ));

        assertNull(parentNode.elements().get("node"));
    }

    /**
     * Tests that conversion from "changer" lambda to a flat map of updates for the storage works properly.
     */
    @Test
    public void flattenedUpdatesMap() {
        var superRoot = new SuperRoot(key -> null, Map.of(ParentConfiguration.KEY, newParentInstance()));

        assertThat(flattenedMap(superRoot, parent -> {}), is(anEmptyMap()));

        assertThat(
            flattenedMap(superRoot, parent -> parent
                .changeElements(elements -> elements
                    .create("name", element -> element
                        .changeChild(child -> child.changeStr("foo"))
                    )
                )
            ),
            is(allOf(
                aMapWithSize(2),
                hasEntry("root.elements.name.child.str", (Serializable)"foo"),
                hasEntry("root.elements.name.<idx>", 0)
            ))
        );

        assertThat(
            flattenedMap(superRoot, parent -> parent
                .changeElements(elements1 -> elements1.delete("void"))
            ),
            is(anEmptyMap())
        );

        assertThat(
            flattenedMap(superRoot, parent -> parent
                .changeElements(elements -> elements.delete("name"))
            ),
            is(allOf(
                aMapWithSize(2),
                hasEntry("root.elements.name.child.str", null),
                hasEntry("root.elements.name.<idx>", null)
            ))
        );
    }

    /**
     * Patches super root and returns flat representation of the changes. Passed {@code superRoot} object will contain
     * patched tree when method execution is completed.
     *
     * @param superRoot Super root to patch.
     * @param patch Closure to cnahge parent node.
     * @return Flat map with all changes from the patch.
     */
    @NotNull private Map<String, Serializable> flattenedMap(SuperRoot superRoot, Consumer<ParentChange> patch) {
        // Preserve a copy of the super root to use it as a golden source of data.
        SuperRoot originalSuperRoot = superRoot.copy();

        // Make a copy of the root insode of the superRoot. This copy will be used for further patching.
        superRoot.construct(ParentConfiguration.KEY.key(), new ConfigurationSource() {});

        // Patch root node.
        patch.accept((ParentChange)superRoot.getRoot(ParentConfiguration.KEY));

        // Create flat diff between two super trees.
        return createFlattenedUpdatesMap(originalSuperRoot, superRoot);
    }

    /**
     * Tests basic invariants of {@link ConfigurationUtil#patch(ConstructableTreeNode, TraversableTreeNode)} method.
     */
    @Test
    public void patch() {
        var originalRoot = newParentInstance();

        originalRoot.changeElements(elements ->
            elements.create("name1", element ->
                element.changeChild(child -> child.changeStr("value1"))
            )
        );

        // Updating config.
        ParentView updatedRoot = ConfigurationUtil.patch(originalRoot, (TraversableTreeNode)copy(originalRoot).changeElements(elements ->
            elements.createOrUpdate("name1", element ->
                element.changeChild(child -> child.changeStr("value2"))
            )
        ));

        assertNotSame(originalRoot, updatedRoot);
        assertNotSame(originalRoot.elements(), updatedRoot.elements());
        assertNotSame(originalRoot.elements().get("name1"), updatedRoot.elements().get("name1"));
        assertNotSame(originalRoot.elements().get("name1").child(), updatedRoot.elements().get("name1").child());

        assertEquals("value1", originalRoot.elements().get("name1").child().str());
        assertEquals("value2", updatedRoot.elements().get("name1").child().str());

        // Expanding config.
        ParentView expandedRoot = ConfigurationUtil.patch(originalRoot, (TraversableTreeNode)copy(originalRoot).changeElements(elements ->
            elements.createOrUpdate("name2", element ->
                element.changeChild(child -> child.changeStr("value2"))
            )
        ));

        assertNotSame(originalRoot, expandedRoot);
        assertNotSame(originalRoot.elements(), expandedRoot.elements());

        assertSame(originalRoot.elements().get("name1"), expandedRoot.elements().get("name1"));
        assertNull(originalRoot.elements().get("name2"));
        assertNotNull(expandedRoot.elements().get("name2"));

        assertEquals("value2", expandedRoot.elements().get("name2").child().str());

        // Shrinking config.
        ParentView shrinkedRoot = (ParentView)ConfigurationUtil.patch((InnerNode)expandedRoot, (TraversableTreeNode)copy(expandedRoot).changeElements(elements ->
            elements.delete("name1")
        ));

        assertNotSame(expandedRoot, shrinkedRoot);
        assertNotSame(expandedRoot.elements(), shrinkedRoot.elements());

        assertNotNull(expandedRoot.elements().get("name1"));
        assertNull(shrinkedRoot.elements().get("name1"));
        assertNotNull(shrinkedRoot.elements().get("name2"));
    }

    /**
     * @param parent {@link ParentView} object.
     * @return Copy of the {@code parent} objects cast to {@link ParentChange}.
     * @see ConstructableTreeNode#copy()
     */
    private ParentChange copy(ParentView parent) {
        return (ParentChange)((ConstructableTreeNode)parent).copy();
    }
}
