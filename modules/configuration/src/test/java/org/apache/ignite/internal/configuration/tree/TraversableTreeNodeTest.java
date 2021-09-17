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

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import org.apache.ignite.configuration.NamedListChange;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Immutable;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** */
public class TraversableTreeNodeTest {
    private static ConfigurationAsmGenerator cgen;

    @BeforeAll
    public static void beforeAll() {
        cgen = new ConfigurationAsmGenerator();

        cgen.compileRootSchema(ParentConfigurationSchema.class, Map.of());
    }

    @AfterAll
    public static void afterAll() {
        cgen = null;
    }

    public static <P extends InnerNode & ParentChange> P newParentInstance() {
        return (P)cgen.instantiateNode(ParentConfigurationSchema.class);
    }

    public static <C extends InnerNode & ChildChange> C newChildInstance() {
        return (C)cgen.instantiateNode(ChildConfigurationSchema.class);
    }

    /** */
    @Config
    public static class ParentConfigurationSchema {
        /** */
        @ConfigValue
        public ChildConfigurationSchema child;

        /** */
        @NamedConfigValue
        public NamedElementConfigurationSchema elements;
    }

    /** */
    @Config
    public static class ChildConfigurationSchema {
        /** */
        @Value(hasDefault = true)
        @Immutable
        public int intCfg = 99;

        /** */
        @Value
        public String strCfg;
    }

    /** */
    @Config
    public static class NamedElementConfigurationSchema {
        /** */
        @Value
        public String strCfg;
    }

    /** */
    private static class VisitException extends RuntimeException {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;
    }

    /**
     * Test that generated node classes implement generated VIEW and CHANGE interfaces.
     */
    @Test
    public void nodeClassesImplementRequiredInterfaces() {
        var parentNode = newParentInstance();

        assertThat(parentNode, instanceOf(ParentView.class));
        assertThat(parentNode, instanceOf(ParentChange.class));

        var namedElementNode = cgen.instantiateNode(NamedElementConfigurationSchema.class);

        assertThat(namedElementNode, instanceOf(NamedElementView.class));
        assertThat(namedElementNode, instanceOf(NamedElementChange.class));

        var childNode = newChildInstance();

        assertThat(childNode, instanceOf(ChildView.class));
        assertThat(childNode, instanceOf(ChildChange.class));
    }

    /**
     * Test for signature and implementation of "change" method on leaves.
     */
    @Test
    public void changeLeaf() {
        var childNode = newChildInstance();

        assertNull(childNode.strCfg());

        childNode.changeStrCfg("value");

        assertEquals("value", childNode.strCfg());

        assertThrows(NullPointerException.class, () -> childNode.changeStrCfg(null));
    }

    /**
     * Test for signature and implementation of "change" method on inner nodes.
     */
    @Test
    public void changeInnerChild() {
        var parentNode = newParentInstance();

        assertNull(parentNode.child());

        parentNode.changeChild(child -> {});

        ChildView childNode = parentNode.child();

        assertNotNull(childNode);

        parentNode.changeChild(child -> child.changeStrCfg("value"));

        assertNotSame(childNode, parentNode.child());

        assertThrows(NullPointerException.class, () -> parentNode.changeChild(null));

        assertThrows(NullPointerException.class, () -> parentNode.changeElements(null));
    }

    /**
     * Test for signature and implementation of "change" method on named list nodes.
     */
    @Test
    public void changeNamedChild() {
        var parentNode = newParentInstance();

        NamedListView<? extends NamedElementView> elementsNode = parentNode.elements();

        // Named list node must always be instantiated.
        assertNotNull(elementsNode);

        parentNode.changeElements(elements -> elements.createOrUpdate("key", element -> {}));

        assertNotSame(elementsNode, parentNode.elements());
    }

    /**
     * Test for signature and implementation of "put" and "remove" methods on elements of named list nodes.
     */
    @Test
    public void putRemoveNamedConfiguration() {
        var elementsNode = (NamedListChange<NamedElementView, NamedElementChange>)newParentInstance().elements();

        assertEquals(List.of(), elementsNode.namedListKeys());

        elementsNode.createOrUpdate("keyPut", element -> {});

        assertThrows(IllegalArgumentException.class, () -> elementsNode.create("keyPut", element -> {}));

        assertThat(elementsNode.namedListKeys(), hasItem("keyPut"));

        NamedElementView elementNode = elementsNode.get("keyPut");

        assertNotNull(elementNode);

        assertSame(elementNode, elementsNode.get(0));

        assertNull(elementNode.strCfg());

        assertThrows(IndexOutOfBoundsException.class, () -> elementsNode.get(-1));

        assertThrows(IndexOutOfBoundsException.class, () -> elementsNode.get(1));

        elementsNode.createOrUpdate("keyPut", element -> element.changeStrCfg("val"));

        // Assert that consecutive put methods create new object every time.
        assertNotSame(elementNode, elementsNode.get("keyPut"));

        elementNode = elementsNode.get("keyPut");

        assertEquals("val", elementNode.strCfg());

        elementsNode.delete("keyPut");

        assertThat(elementsNode.namedListKeys(), CoreMatchers.hasItem("keyPut"));

        assertNull(elementsNode.get("keyPut"));

        elementsNode.delete("keyPut");

        // Assert that "remove" method creates null element inside of the node.
        assertThat(elementsNode.namedListKeys(), hasItem("keyPut"));

        assertNull(elementsNode.get("keyPut"));

        // Assert that once you remove something from list, you can't put it back again with different set of fields.
        assertThrows(IllegalArgumentException.class, () -> elementsNode.createOrUpdate("keyPut", element -> {}));
    }

    /**
     * Test that inner nodes properly implement visitor interface.
     */
    @Test
    public void innerNodeAcceptVisitor() {
        var parentNode = newParentInstance();

        assertThrows(VisitException.class, () ->
            parentNode.accept("root", new ConfigurationVisitor<Void>() {
                @Override public Void visitInnerNode(String key, InnerNode node) {
                    throw new VisitException();
                }
            })
        );
    }

    /**
     * Test that named list nodes properly implement visitor interface.
     */
    @Test
    public void namedListNodeAcceptVisitor() {
        var elementsNode = (TraversableTreeNode)newParentInstance().elements();

        assertThrows(VisitException.class, () ->
            elementsNode.accept("root", new ConfigurationVisitor<Void>() {
                @Override public <N extends InnerNode> Void visitNamedListNode(String key, NamedListNode<N> node) {
                    throw new VisitException();
                }
            })
        );
    }

    /**
     * Test for "traverseChildren" method implementation on generated inner nodes classes.
     */
    @Test
    public void traverseChildren() {
        var parentNode = newParentInstance();

        Collection<String> keys = new TreeSet<>();

        parentNode.traverseChildren(new ConfigurationVisitor<Object>() {
            @Override public Object visitInnerNode(String key, InnerNode node) {
                assertNull(node);

                assertEquals("child", key);

                return keys.add(key);
            }

            @Override public <N extends InnerNode> Object visitNamedListNode(String key, NamedListNode<N> node) {
                assertEquals("elements", key);

                return keys.add(key);
            }
        }, true);

        // Assert that updates happened in the same order as fields declaration in schema.
        assertEquals(new TreeSet<>(List.of("child", "elements")), keys);

        keys.clear();

        var childNode = newChildInstance();

        childNode.traverseChildren(new ConfigurationVisitor<Object>() {
            @Override public Object visitLeafNode(String key, Serializable val) {
                return keys.add(key);
            }
        }, true);

        // Assert that updates happened in the same order as fields declaration in schema.
        assertEquals(new TreeSet<>(List.of("intCfg", "strCfg")), keys);
    }

    /**
     * Test for "traverseChild" method implementation on generated inner nodes classes.
     */
    @Test
    public void traverseSingleChild() {
        var parentNode = newParentInstance();

        // Assert that proper method has been invoked.
        assertThrows(VisitException.class, () ->
            parentNode.traverseChild("child", new ConfigurationVisitor<Void>() {
                @Override public Void visitInnerNode(String key, InnerNode node) {
                    assertEquals("child", key);

                    throw new VisitException();
                }
            }, true)
        );

        // Assert that proper method has been invoked.
        assertThrows(VisitException.class, () ->
            parentNode.traverseChild("elements", new ConfigurationVisitor<Void>() {
                @Override
                public <N extends InnerNode> Void visitNamedListNode(String key, NamedListNode<N> node) {
                    assertEquals("elements", key);

                    throw new VisitException();
                }
            }, true)
        );

        var childNode = newChildInstance();

        // Assert that proper method has been invoked.
        assertThrows(VisitException.class, () ->
            childNode.traverseChild("intCfg", new ConfigurationVisitor<Void>() {
                @Override public Void visitLeafNode(String key, Serializable val) {
                    assertEquals("intCfg", key);

                    throw new VisitException();
                }
            }, true)
        );

        // Assert that traversing inexistent field leads to exception.
        assertThrows(NoSuchElementException.class, () ->
            childNode.traverseChild("foo", new ConfigurationVisitor<>() {}, true)
        );
    }
}
