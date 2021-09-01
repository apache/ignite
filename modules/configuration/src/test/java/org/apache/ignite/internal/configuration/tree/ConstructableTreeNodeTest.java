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

package org.apache.ignite.internal.configuration.tree;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** */
public class ConstructableTreeNodeTest {
    private static ConfigurationAsmGenerator cgen;

    @BeforeAll
    public static void beforeAll() {
        cgen = new ConfigurationAsmGenerator();

        cgen.compileRootSchema(TraversableTreeNodeTest.ParentConfigurationSchema.class, Map.of());
    }

    @AfterAll
    public static void afterAll() {
        cgen = null;
    }

    public static <P extends InnerNode & ParentChange> P newParentInstance() {
        return (P)cgen.instantiateNode(TraversableTreeNodeTest.ParentConfigurationSchema.class);
    }

    public static <C extends InnerNode & ChildChange> C newChildInstance() {
        return (C)cgen.instantiateNode(TraversableTreeNodeTest.ChildConfigurationSchema.class);
    }

    /** */
    @Test
    public void noKey() {
        var childNode = newChildInstance();

        assertThrows(NoSuchElementException.class, () -> childNode.construct("foo", null, true));
    }

    /** */
    @Test
    public void nullSource() {
        var parentNode = newParentInstance();

        parentNode.changeChild(child ->
            child.changeStrCfg("value")
        )
        .changeElements(elements ->
            elements.create("name", element -> {})
        );

        // Named list node.
        var elements = parentNode.elements();

        parentNode.construct("elements", null, true);

        assertNotNull(parentNode.elements());
        assertNotSame(elements, parentNode.elements());
        assertEquals(List.of(), parentNode.elements().namedListKeys());

        // Inner node.
        NamedElementView element = elements.get("name");

        ((ConstructableTreeNode)elements).construct("name", null, true);

        assertNull(elements.get("name"));

        // Leaf.
        ((ConstructableTreeNode)element).construct("strCfg", null, true);

        assertNull(element.strCfg());
    }

    /** */
    private static class ConstantConfigurationSource implements ConfigurationSource {
        /** */
        private final Object constant;

        /**
         * @param constant Constant.
         */
        private ConstantConfigurationSource(Object constant) {
            this.constant = constant;
        }

        /** {@inheritDoc} */
        @Override public <T> T unwrap(Class<T> clazz) {
            return clazz.cast(constant);
        }
    }

    /** */
    @Test
    public void unwrap() {
        var childNode = newChildInstance();

        childNode.construct("strCfg", new ConstantConfigurationSource("value"), true);

        assertEquals("value", childNode.strCfg());

        childNode.construct("intCfg", new ConstantConfigurationSource(255), true);

        assertEquals(255, childNode.intCfg());

        assertThrows(ClassCastException.class, () ->
            childNode.construct("intCfg", new ConstantConfigurationSource(new Object()), true)
        );
    }

    /** */
    @Test
    public void descend() {
        // Inner node.
        var parentNode = newParentInstance();

        parentNode.construct("child", new ConfigurationSource() {
            @Override public <T> T unwrap(Class<T> clazz) {
                throw new UnsupportedOperationException("unwrap");
            }

            @Override public void descend(ConstructableTreeNode node) {
                node.construct("strCfg", new ConstantConfigurationSource("value"), true);
            }
        }, true);

        assertEquals("value", parentNode.child().strCfg());

        // Named list node.
        var elementsNode = parentNode.elements();

        ((ConstructableTreeNode)elementsNode).construct("name", new ConfigurationSource() {
            @Override public <T> T unwrap(Class<T> clazz) {
                throw new UnsupportedOperationException("unwrap");
            }

            @Override public void descend(ConstructableTreeNode node) {
                node.construct("strCfg", new ConstantConfigurationSource("value"), true);
            }
        }, true);

        assertEquals("value", elementsNode.get("name").strCfg());
    }

    /** */
    @Test
    public void constructDefault() {
        // Inner node with no leaves.
        var parentNode = newParentInstance();

        assertThrows(NoSuchElementException.class, () -> parentNode.constructDefault("child"));
        assertThrows(NoSuchElementException.class, () -> parentNode.constructDefault("elements"));

        // Inner node with a leaf.
        parentNode.changeElements(elements -> elements.create("name", element -> {}));

        var elementNode = parentNode.elements().get("name");

        ((InnerNode)elementNode).constructDefault("strCfg");

        assertNull(elementNode.strCfg());

        // Another inner node with leaves.
        parentNode.changeChild(child -> {});

        var child = parentNode.child();

        ((InnerNode)child).constructDefault("strCfg");

        assertThrows(NullPointerException.class, () -> child.intCfg());

        ((InnerNode)child).constructDefault("intCfg");

        assertEquals(99, child.intCfg());
    }
}
