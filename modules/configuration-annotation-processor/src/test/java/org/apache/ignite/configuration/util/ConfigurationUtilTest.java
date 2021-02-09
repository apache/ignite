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

package org.apache.ignite.configuration.util;

import java.util.List;
import java.util.Map;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.util.impl.ParentNode;
import org.junit.jupiter.api.Test;

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** */
public class ConfigurationUtilTest {
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
    @Config
    public static class ParentConfigurationSchema {
        /** */
        @NamedConfigValue
        private NamedElementConfigurationSchema elements;
    }

    /** */
    @Config
    public static class NamedElementConfigurationSchema {
        /** */
        @ConfigValue
        private ChildConfigurationSchema child;
    }

    /** */
    @Config
    public static class ChildConfigurationSchema {
        /** */
        @Value
        private String str;
    }

    /** */
    @Test
    public void findSuccessfully() {
        var parent = new ParentNode().changeElements(elements ->
            elements.put("name", element ->
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
        var parent = new ParentNode();

        assertNull(ConfigurationUtil.find(List.of("elements", "name"), parent));

        parent.changeElements(elements -> elements.put("name", element -> {}));

        assertNull(ConfigurationUtil.find(List.of("elements", "name", "child"), parent));

        parent.elements().get("name").changeChild(child -> {});

        assertNull(ConfigurationUtil.find(List.of("elements", "name", "child", "str"), parent));
    }

    /** */
    @Test
    public void findUnsuccessfully() {
        var parent = new ParentNode();

        assertThrows(
            KeyNotFoundException.class,
            () -> ConfigurationUtil.find(List.of("elements", "name", "child"), parent)
        );

        parent.changeElements(elements -> elements.put("name", element -> {}));

        assertThrows(
            KeyNotFoundException.class,
            () -> ConfigurationUtil.find(List.of("elements", "name", "child", "str"), parent)
        );

        parent.elements().get("name").changeChild(child -> child.changeStr("value"));

        assertThrows(
            KeyNotFoundException.class,
            () -> ConfigurationUtil.find(List.of("elements", "name", "child", "str", "foo"), parent)
        );
    }

    /** */
    @Test
    public void fillFromSuffixMapSuccessfully() {
        var parentNode = new ParentNode();

        ConfigurationUtil.fillFromSuffixMap(parentNode, Map.of(
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
    public void fillFromSuffixMapSuccessfullyWithRemove() {
        var parentNode = new ParentNode().changeElements(elements ->
            elements.put("name", element ->
                element.changeChild(child -> {})
            )
        );

        ConfigurationUtil.fillFromSuffixMap(parentNode, Map.of(
            "elements", singletonMap("name", null)
        ));

        assertNull(parentNode.elements().get("node"));
    }

    /** */
    @Test
    public void patch() {
        var originalRoot = new ParentNode().initElements(elements ->
            elements.put("name1", element ->
                element.initChild(child -> child.initStr("value1"))
            )
        );

        // Updating config.
        ParentNode updatedRoot = ConfigurationUtil.patch(originalRoot, new ParentNode().changeElements(elements ->
            elements.put("name1", element ->
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
        ParentNode expandedRoot = ConfigurationUtil.patch(originalRoot, new ParentNode().changeElements(elements ->
            elements.put("name2", element ->
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
        ParentNode shrinkedRoot = ConfigurationUtil.patch(expandedRoot, new ParentNode().changeElements(elements ->
            elements.remove("name1")
        ));

        assertNotSame(expandedRoot, shrinkedRoot);
        assertNotSame(expandedRoot.elements(), shrinkedRoot.elements());

        assertNotNull(expandedRoot.elements().get("name1"));
        assertNull(shrinkedRoot.elements().get("name1"));
        assertNotNull(shrinkedRoot.elements().get("name2"));
    }
}
