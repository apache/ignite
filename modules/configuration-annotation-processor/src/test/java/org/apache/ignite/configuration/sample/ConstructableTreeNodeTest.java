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

package org.apache.ignite.configuration.sample;

import java.util.Collections;
import java.util.NoSuchElementException;
import org.apache.ignite.configuration.sample.impl.ChildNode;
import org.apache.ignite.configuration.sample.impl.NamedElementNode;
import org.apache.ignite.configuration.sample.impl.ParentNode;
import org.apache.ignite.configuration.tree.ConfigurationSource;
import org.apache.ignite.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.configuration.tree.NamedListNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** */
public class ConstructableTreeNodeTest {
    /** */
    @Test
    public void noKey() {
        var childNode = new ChildNode();

        assertThrows(NoSuchElementException.class, () -> childNode.construct("foo", null));
    }

    /** */
    @Test
    public void nullSource() {
        var parentNode = new ParentNode().initChild(child ->
            child.initStrCfg("value")
        )
        .initElements(elements ->
            elements.create("name", element -> {})
        );

        // Named list node.
        NamedListNode<NamedElementNode> elements = parentNode.elements();

        parentNode.construct("elements", null);

        assertNotNull(parentNode.elements());
        assertNotSame(elements, parentNode.elements());
        assertEquals(Collections.emptySet(), parentNode.elements().namedListKeys());

        // Inner node.
        NamedElementNode element = elements.get("name");

        elements.construct("name", null);

        assertNull(elements.get("name"));

        // Leaf.
        element.construct("strCfg", null);

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
        var childNode = new ChildNode();

        childNode.construct("strCfg", new ConstantConfigurationSource("value"));

        assertEquals("value", childNode.strCfg());

        childNode.construct("intCfg", new ConstantConfigurationSource(255));

        assertEquals(255, childNode.intCfg());

        assertThrows(ClassCastException.class, () ->
            childNode.construct("intCfg", new ConstantConfigurationSource(new Object()))
        );
    }

    /** */
    @Test
    public void descend() {
        // Inner node.
        var parentNode = new ParentNode();

        parentNode.construct("child", new ConfigurationSource() {
            @Override public <T> T unwrap(Class<T> clazz) {
                throw new UnsupportedOperationException("unwrap");
            }

            @Override public void descend(ConstructableTreeNode node) {
                node.construct("strCfg", new ConstantConfigurationSource("value"));
            }
        });

        assertEquals("value", parentNode.child().strCfg());

        // Named list node.
        NamedListNode<NamedElementNode> elementsNode = parentNode.elements();

        elementsNode.construct("name", new ConfigurationSource() {
            @Override public <T> T unwrap(Class<T> clazz) {
                throw new UnsupportedOperationException("unwrap");
            }

            @Override public void descend(ConstructableTreeNode node) {
                node.construct("strCfg", new ConstantConfigurationSource("value"));
            }
        });

        assertEquals("value", elementsNode.get("name").strCfg());
    }

    /** */
    @Test
    public void constructDefault() {
        // Inner node with no leaves.
        var parentNode = new ParentNode();

        assertThrows(NoSuchElementException.class, () -> parentNode.constructDefault("child"));
        assertThrows(NoSuchElementException.class, () -> parentNode.constructDefault("elements"));

        // Inner node with a leaf.
        parentNode.changeElements(elements -> elements.create("name", element -> {}));

        NamedElementNode elementNode = parentNode.elements().get("name");

        assertFalse(elementNode.constructDefault("strCfg"));

        // Another inner node with leaves.
        parentNode.changeChild(child -> {});

        ChildNode child = parentNode.child();

        assertFalse(child.constructDefault("strCfg"));

        assertThrows(NullPointerException.class, () -> child.intCfg());

        assertTrue(child.constructDefault("intCfg"));

        assertEquals(99, child.intCfg());
    }
}
