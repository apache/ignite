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

package org.apache.ignite.configuration.internal.util;

import java.util.List;
import java.util.Map;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.internal.SuperRoot;
import org.apache.ignite.configuration.internal.asm.ConfigurationAsmGenerator;
import org.apache.ignite.configuration.storage.ConfigurationType;
import org.apache.ignite.configuration.tree.InnerNode;
import org.apache.ignite.configuration.tree.TraversableTreeNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
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

    public static <P extends InnerNode & ParentView & ParentChange> P newParentInstance() {
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

    /** */
    @Test
    public void findSuccessfully() {
        var parent = newParentInstance();

        parent.changeElements(elements ->
            elements.update("name", element ->
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

    /** */
    @Test
    public void findNulls() {
        var parent = newParentInstance();

        assertNull(ConfigurationUtil.find(List.of("elements", "name"), parent));

        parent.changeElements(elements -> elements.update("name", element -> {}));

        assertNull(ConfigurationUtil.find(List.of("elements", "name", "child"), parent));

        ((NamedElementChange)parent.elements().get("name")).changeChild(child -> {});

        assertNull(ConfigurationUtil.find(List.of("elements", "name", "child", "str"), parent));
    }

    /** */
    @Test
    public void findUnsuccessfully() {
        var parent = newParentInstance();

        assertThrows(
            KeyNotFoundException.class,
            () -> ConfigurationUtil.find(List.of("elements", "name", "child"), parent)
        );

        parent.changeElements(elements -> elements.update("name", element -> {}));

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

    /** */
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

    /** */
    @Test
    public void fillFromPrefixMapSuccessfully() {
        var parentNode = newParentInstance();

        ConfigurationUtil.fillFromPrefixMap(parentNode, Map.of(
            "elements", Map.of(
                "name1", Map.of(
                    "child", Map.of("str", "value1")
                ),
                "name2", Map.of(
                    "child", Map.of("str", "value2")
                )
            )
        ));

        assertEquals("value1", parentNode.elements().get("name1").child().str());
        assertEquals("value2", parentNode.elements().get("name2").child().str());
    }

    /** */
    @Test
    public void fillFromPrefixMapSuccessfullyWithRemove() {
        var parentNode = newParentInstance();

        parentNode.changeElements(elements ->
            elements.update("name", element ->
                element.changeChild(child -> {})
            )
        );

        ConfigurationUtil.fillFromPrefixMap(parentNode, Map.of(
            "elements", singletonMap("name", null)
        ));

        assertNull(parentNode.elements().get("node"));
    }

    /** */
    @Test
    public void nodeToFlatMap() {
        var parentNode = newParentInstance();

        assertEquals(
            emptyMap(),
            ConfigurationUtil.nodeToFlatMap(null, new SuperRoot(key -> null, Map.of(
                ParentConfiguration.KEY,
                parentNode
            )))
        );

        // No defaults in this test so everything must be initialized explicitly.
        parentNode.changeElements(elements ->
            elements.create("name", element ->
                element.changeChild(child ->
                    child.changeStr("foo")
                )
            )
        );

        assertEquals(
            singletonMap("root.elements.name.child.str", "foo"),
            ConfigurationUtil.nodeToFlatMap(null, new SuperRoot(key -> null, Map.of(
                ParentConfiguration.KEY,
                parentNode
            )))
        );

        assertEquals(
            emptyMap(),
            ConfigurationUtil.nodeToFlatMap(new SuperRoot(key -> null, Map.of(
                ParentConfiguration.KEY,
                parentNode
            )), new SuperRoot(key -> null, singletonMap(
                ParentConfiguration.KEY,
                (InnerNode)newParentInstance().changeElements(elements ->
                    elements.delete("void")
                )
            )))
        );

        assertEquals(
            singletonMap("root.elements.name.child.str", null),
            ConfigurationUtil.nodeToFlatMap(new SuperRoot(key -> null, Map.of(
                ParentConfiguration.KEY,
                parentNode
            )), new SuperRoot(key -> null, singletonMap(
                ParentConfiguration.KEY,
                (InnerNode)newParentInstance().changeElements(elements ->
                    elements.delete("name")
                )
            )))
        );
    }

    /** */
    @Test
    public void patch() {
        var originalRoot = newParentInstance();

        originalRoot.changeElements(elements ->
            elements.create("name1", element ->
                element.changeChild(child -> child.changeStr("value1"))
            )
        );

        // Updating config.
        ParentView updatedRoot = ConfigurationUtil.patch(originalRoot, (TraversableTreeNode)newParentInstance().changeElements(elements ->
            elements.update("name1", element ->
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
        ParentView expandedRoot = ConfigurationUtil.patch(originalRoot, (TraversableTreeNode)newParentInstance().changeElements(elements ->
            elements.update("name2", element ->
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
        ParentView shrinkedRoot = (ParentView)ConfigurationUtil.patch((InnerNode)expandedRoot, (TraversableTreeNode)newParentInstance().changeElements(elements ->
            elements.delete("name1")
        ));

        assertNotSame(expandedRoot, shrinkedRoot);
        assertNotSame(expandedRoot.elements(), shrinkedRoot.elements());

        assertNotNull(expandedRoot.elements().get("name1"));
        assertNull(shrinkedRoot.elements().get("name1"));
        assertNotNull(shrinkedRoot.elements().get("name2"));
    }

    /** */
    @Test
    public void cleanupMatchingValues() {
        var curParent = newParentInstance();

        curParent.changeElements(elements -> elements
            .create("missing", element -> {})
            .create("match", element -> element.changeChild(child -> child.changeStr("match")))
            .create("mismatch", element -> element.changeChild(child -> child.changeStr("foo")))
        );

        var newParent = newParentInstance();

        newParent.changeElements(elements -> elements
            .create("extra", element -> {})
            .create("match", element -> element.changeChild(child -> child.changeStr("match")))
            .create("mismatch", element -> element.changeChild(child -> child.changeStr("bar")))
        );

        ConfigurationUtil.cleanupMatchingValues(curParent, newParent);

        // Old node stayed intact.
        assertEquals("match", curParent.elements().get("match").child().str());
        assertEquals("foo", curParent.elements().get("mismatch").child().str());

        // New node was modified.
        assertNull(newParent.elements().get("match").child().str());
        assertEquals("bar", newParent.elements().get("mismatch").child().str());
    }
}
