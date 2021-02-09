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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.RandomAccess;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.tree.ConfigurationSource;
import org.apache.ignite.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.configuration.tree.InnerNode;
import org.apache.ignite.configuration.tree.NamedListNode;
import org.apache.ignite.configuration.tree.TraversableTreeNode;

/** */
public class ConfigurationUtil {
    /**
     * Replaces all {@code .} and {@code \} characters with {@code \.} and {@code \\} respectively.
     *
     * @param key Unescaped string.
     * @return Escaped string.
     */
    public static String escape(String key) {
        return key.replaceAll("([.\\\\])", "\\\\$1");
    }

    /**
     * Replaces all {@code \.} and {@code \\} with {@code .} and {@code \} respectively.
     *
     * @param key Escaped string.
     * @return Unescaped string.
     */
    public static String unescape(String key) {
        return key.replaceAll("\\\\([.\\\\])", "$1");
    }

    /**
     * Splits string using unescaped {@code .} character as a separator.
     *
     * @param keys Qualified key where escaped subkeys are joined with dots.
     * @return List of unescaped subkeys.
     * @see #unescape(String)
     * @see #join(List)
     */
    public static List<String> split(String keys) {
        String[] split = keys.split("(?<!\\\\)[.]", -1);

        for (int i = 0; i < split.length; i++)
            split[i] = unescape(split[i]);

        return Arrays.asList(split);
    }

    /**
     * Joins list of keys with {@code .} character as a separator. All keys are preemptively escaped.
     *
     * @param keys List of unescaped keys.
     * @return Escaped keys joined with dots.
     * @see #escape(String)
     * @see #split(String)
     */
    public static String join(List<String> keys) {
        return keys.stream().map(ConfigurationUtil::escape).collect(Collectors.joining("."));
    }

    /**
     * Search for the configuration node by the list of keys.
     *
     * @param keys Random access list with keys.
     * @param node Node where method will search for subnode.
     * @return Either {@link TraversableTreeNode} or {@link Serializable} depending on the keys and schema.
     * @throws KeyNotFoundException If node is not found.
     */
    public static Object find(List<String> keys, TraversableTreeNode node) throws KeyNotFoundException {
        assert keys instanceof RandomAccess : keys.getClass();

        var visitor = new ConfigurationVisitor() {
            /** */
            private int i;

            /** */
            private Object res;

            @Override public void visitLeafNode(String key, Serializable val) {
                if (i != keys.size())
                    throw new KeyNotFoundException("Configuration value '" + join(keys.subList(0, i)) + "' is a leaf");

                res = val;
            }

            @Override public void visitInnerNode(String key, InnerNode node) {
                if (i == keys.size())
                    res = node;
                else if (node == null)
                    throw new KeyNotFoundException("Configuration node '" + join(keys.subList(0, i)) + "' is null");
                else {
                    try {
                        node.traverseChild(keys.get(i++), this);
                    }
                    catch (NoSuchElementException e) {
                        throw new KeyNotFoundException("Configuration '" + join(keys.subList(0, i)) + "' is not found");
                    }
                }
            }

            @Override public <N extends InnerNode> void visitNamedListNode(String key, NamedListNode<N> node) {
                if (i == keys.size())
                    res = node;
                else {
                    String name = keys.get(i++);

                    visitInnerNode(name, node.get(name));
                }
            }
        };

        node.accept(null, visitor);

        return visitor.res;
    }

    /**
     * Convert Map tree to configuration tree. No error handling here.
     *
     * @param node Node to fill. Not necessarily empty.
     * @param prefixMap Map of {@link Serializable} values or other prefix maps (recursive structure).
     *      Every key is unescaped.
     * @throws UnsupportedOperationException if prefix map structure doesn't correspond to actual tree structure.
     *      This will be fixed when method is actually used in configuration storage intergration.
     */
    public static void fillFromSuffixMap(ConstructableTreeNode node, Map<String, ?> prefixMap) {
        assert node instanceof InnerNode;

        /** */
        class LeafConfigurationSource implements ConfigurationSource {
            /** */
            private final Serializable val;

            /**
             * @param val Value.
             */
            private LeafConfigurationSource(Serializable val) {
                this.val = val;
            }

            /** {@inheritDoc} */
            @Override public <T> T unwrap(Class<T> clazz) {
                assert val == null || clazz.isInstance(val);

                return clazz.cast(val);
            }

            /** {@inheritDoc} */
            @Override public void descend(ConstructableTreeNode node) {
                throw new UnsupportedOperationException("descend");
            }
        }

        /** */
        class InnerConfigurationSource implements ConfigurationSource {
            /** */
            private final Map<String, ?> map;

            /**
             * @param map Prefix map.
             */
            private InnerConfigurationSource(Map<String, ?> map) {
                this.map = map;
            }

            /** {@inheritDoc} */
            @Override public <T> T unwrap(Class<T> clazz) {
                throw new UnsupportedOperationException("unwrap");
            }

            /** {@inheritDoc} */
            @Override public void descend(ConstructableTreeNode node) {
                for (Map.Entry<String, ?> entry : map.entrySet()) {
                    String key = entry.getKey();
                    Object val = entry.getValue();

                    assert val == null || val instanceof Map || val instanceof Serializable;

                    if (val == null)
                        node.construct(key, null);
                    else if (val instanceof Map)
                        node.construct(key, new InnerConfigurationSource((Map<String, ?>)val));
                    else
                        node.construct(key, new LeafConfigurationSource((Serializable)val));
                }
            }
        }

        var src = new InnerConfigurationSource(prefixMap);

        src.descend(node);
    }

    /**
     * Apply changes on top of existing node. Creates completely new object while reusing parts of the original tree
     * that weren't modified.
     *
     * @param root Immutable configuration node.
     * @param changes Change or Init object to be applied.
     */
    public static <C extends ConstructableTreeNode> C patch(C root, TraversableTreeNode changes) {
        assert root.getClass() == changes.getClass(); // Yes.

        var scrHolder = new ConfigurationVisitor() {
            ConfigurationSource src;

            @Override public void visitInnerNode(String key, InnerNode node) {
                src = new PatchInnerConfigurationSource(node);
            }

            @Override public <N extends InnerNode> void visitNamedListNode(String key, NamedListNode<N> node) {
                src = new PatchNamedListConfigurationSource(node);
            }
        };

        changes.accept(null, scrHolder);

        assert scrHolder.src != null;

        C copy = (C)root.copy();

        scrHolder.src.descend(copy);

        return copy;
    }

    /** */
    private static class PatchLeafConfigurationSource implements ConfigurationSource {
        /** */
        private final Serializable val;

        /**
         * @param val Value.
         */
        PatchLeafConfigurationSource(Serializable val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public <T> T unwrap(Class<T> clazz) {
            assert clazz.isInstance(val);

            return clazz.cast(val);
        }

        /** {@inheritDoc} */
        @Override public void descend(ConstructableTreeNode node) {
            throw new UnsupportedOperationException("descend");
        }
    }

    /** */
    private static class PatchInnerConfigurationSource implements ConfigurationSource {
        /** */
        private final InnerNode srcNode;

        /**
         * @param srcNode Inner node.
         */
        PatchInnerConfigurationSource(InnerNode srcNode) {
            this.srcNode = srcNode;
        }

        /** {@inheritDoc} */
        @Override public <T> T unwrap(Class<T> clazz) {
            throw new UnsupportedOperationException("unwrap");
        }

        /** {@inheritDoc} */
        @Override public void descend(ConstructableTreeNode dstNode) {
            assert srcNode.getClass() == dstNode.getClass();

            srcNode.traverseChildren(new ConfigurationVisitor() {
                @Override public void visitLeafNode(String key, Serializable val) {
                    if (val != null)
                        dstNode.construct(key, new PatchLeafConfigurationSource(val));
                }

                @Override public void visitInnerNode(String key, InnerNode node) {
                    if (node != null)
                        dstNode.construct(key, new PatchInnerConfigurationSource(node));
                }

                @Override public <N extends InnerNode> void visitNamedListNode(String key, NamedListNode<N> node) {
                    if (node != null)
                        dstNode.construct(key, new PatchNamedListConfigurationSource(node));
                }
            });
        }
    }

    /** */
    private static class PatchNamedListConfigurationSource implements ConfigurationSource {
        /** */
        private final NamedListNode<?> srcNode;

        /**
         * @param srcNode Named list node.
         */
        PatchNamedListConfigurationSource(NamedListNode<?> srcNode) {
            this.srcNode = srcNode;
        }

        /** {@inheritDoc} */
        @Override public <T> T unwrap(Class<T> clazz) {
            throw new UnsupportedOperationException("unwrap");
        }

        /** {@inheritDoc} */
        @Override public void descend(ConstructableTreeNode dstNode) {
            assert srcNode.getClass() == dstNode.getClass();

            for (String key : srcNode.namedListKeys()) {
                InnerNode node = srcNode.get(key);

                dstNode.construct(key, node == null ? null : new PatchInnerConfigurationSource(node));
            }
        }
    }
}
