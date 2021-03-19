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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.RandomAccess;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.internal.SuperRoot;
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
     * @return Random access list of unescaped subkeys.
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

        var visitor = new ConfigurationVisitor<>() {
            /** */
            private int i;

            @Override public Object visitLeafNode(String key, Serializable val) {
                if (i != keys.size())
                    throw new KeyNotFoundException("Configuration value '" + join(keys.subList(0, i)) + "' is a leaf");

                return val;
            }

            @Override public Object visitInnerNode(String key, InnerNode node) {
                if (i == keys.size())
                    return node;
                else if (node == null)
                    throw new KeyNotFoundException("Configuration node '" + join(keys.subList(0, i)) + "' is null");
                else {
                    try {
                        return node.traverseChild(keys.get(i++), this);
                    }
                    catch (NoSuchElementException e) {
                        throw new KeyNotFoundException("Configuration '" + join(keys.subList(0, i)) + "' is not found");
                    }
                }
            }

            @Override public <N extends InnerNode> Object visitNamedListNode(String key, NamedListNode<N> node) {
                if (i == keys.size())
                    return node;
                else {
                    String name = keys.get(i++);

                    return visitInnerNode(name, node.get(name));
                }
            }
        };

        return node.accept(null, visitor);
    }

    /**
     * Converts raw map with dot-separated keys into a prefix map.
     *
     * @param rawConfig Original map.
     * @return Prefix map.
     * @see #split(String)
     */
    public static Map<String, ?> toPrefixMap(Map<String, Serializable> rawConfig) {
        Map<String, Object> res = new HashMap<>();

        for (Map.Entry<String, Serializable> entry : rawConfig.entrySet()) {
            List<String> keys = split(entry.getKey());

            assert keys instanceof RandomAccess : keys.getClass();

            insert(res, keys, 0, entry.getValue());
        }

        return res;
    }

    /**
     * Inserts value into the prefix by a given "path".
     *
     * @param map Output map.
     * @param keys List of keys.
     * @param idx Starting position in the {@code keys} list.
     * @param val Value to be inserted.
     */
    private static void insert(Map<String, Object> map, List<String> keys, int idx, Serializable val) {
        String key = keys.get(idx);

        if (keys.size() == idx + 1) {
            assert !map.containsKey(key) : map.get(key);

            map.put(key, val);
        }
        else {
            Object node = map.get(key);

            Map<String, Object> submap;

            if (node == null) {
                submap = new HashMap<>();

                map.put(key, submap);
            }
            else {
                assert node instanceof Map : node;

                submap = (Map<String, Object>)node;
            }

            insert(submap, keys, idx + 1, val);
        }
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
    public static void fillFromPrefixMap(ConstructableTreeNode node, Map<String, ?> prefixMap) {
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
                    else {
                        assert val instanceof Serializable;

                        node.construct(key, new LeafConfigurationSource((Serializable)val));
                    }
                }
            }
        }

        var src = new InnerConfigurationSource(prefixMap);

        src.descend(node);
    }

    /**
     * Convert a traversable tree to a map of qualified keys to values.
     *
     * @param rootKey Root configuration key.
     * @param curRoot Current root tree.
     * @param updates Tree with updates.
     * @return Map of changes.
     */
    public static Map<String, Serializable> nodeToFlatMap(
        SuperRoot curRoots,
        SuperRoot updates
    ) {
        Map<String, Serializable> values = new HashMap<>();

        updates.traverseChildren(new KeysTrackingConfigurationVisitor<>() {
            /** Write nulls instead of actual values. Makes sense for deletions from named lists. */
            private boolean writeNulls;

            /** {@inheritDoc} */
            @Override public Map<String, Serializable> doVisitLeafNode(String key, Serializable val) {
                if (val != null)
                    values.put(currentKey(), writeNulls ? null : val);

                return values;
            }

            /** {@inheritDoc} */
            @Override public Map<String, Serializable> doVisitInnerNode(String key, InnerNode node) {
                if (node == null)
                    return null;

                node.traverseChildren(this);

                return values;
            }

            /** {@inheritDoc} */
            @Override public <N extends InnerNode> Map<String, Serializable> doVisitNamedListNode(String key, NamedListNode<N> node) {
                for (String namedListKey : node.namedListKeys()) {
                    N namedElement = node.get(namedListKey);

                    withTracking(namedListKey, true, false, () -> {
                        if (namedElement == null)
                            visitDeletedNamedListElement();
                        else
                            namedElement.traverseChildren(this);

                        return null;
                    });
                }

                return values;
            }

            /**
             * Here we must list all joined keys belonging to deleted element. The only way to do it is to traverse
             * cached configuration tree and write {@code null} into all values.
             */
            private void visitDeletedNamedListElement() {
                // It must be impossible to delete something inside of the element that we're currently deleting.
                assert !writeNulls;

                Object originalNamedElement = null;

                List<String> currentPath = currentPath();

                try {
                    // This code can in fact be better optimized for deletion scenario,
                    // but there's no point in doing that, since the operation is so rare and it will
                    // complicate code even more.
                    originalNamedElement = find(currentPath, curRoots);
                }
                catch (KeyNotFoundException ignore) {
                    // May happen, not a big deal. This means that element never existed in the first place.
                }

                if (originalNamedElement != null) {
                    assert originalNamedElement instanceof InnerNode : currentPath;

                    writeNulls = true;

                    ((InnerNode)originalNamedElement).traverseChildren(this);

                    writeNulls = false;
                }
            }
        });

        return values;
    }

    /**
     * Creates new list that is a conjunction of given list and element.
     *
     * @param prefix Head of the new list.
     * @param key Tail element of the new list.
     * @return New list.
     */
    public static List<String> appendKey(List<String> prefix, String key) {
        if (prefix.isEmpty())
            return List.of(key);

        List<String> res = new ArrayList<>(prefix.size() + 1);
        res.addAll(prefix);
        res.add(key);

        return res;
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

        var scrVisitor = new ConfigurationVisitor<ConfigurationSource>() {
            @Override public ConfigurationSource visitInnerNode(String key, InnerNode node) {
                return new PatchInnerConfigurationSource(node);
            }

            @Override public <N extends InnerNode> ConfigurationSource visitNamedListNode(String key, NamedListNode<N> node) {
                return new PatchNamedListConfigurationSource(node);
            }
        };

        ConfigurationSource src = changes.accept(null, scrVisitor);

        assert src != null;

        C copy = (C)root.copy();

        src.descend(copy);

        return copy;
    }

    /**
     * Fill {@code dst} node with default values, required to complete {@code src} node.
     * These two objects can be the same, this would mean that all {@code null} values of {@code scr} will be
     * replaced with defaults if it's possible.
     *
     * @param src Source node.
     * @param dst Destination node.
     */
    public static void addDefaults(InnerNode src, InnerNode dst) {
        assert src.getClass() == dst.getClass();

        src.traverseChildren(new ConfigurationVisitor<>() {
            @Override public Object visitLeafNode(String key, Serializable val) {
                // If source value is null then inititalise the same value on the destination node.
                if (val == null)
                    dst.constructDefault(key);

                return null;
            }

            @Override public Object visitInnerNode(String key, InnerNode srcNode) {
                // Instantiate field in destination node before doing something else.
                // Not a big deal if it wasn't null.
                dst.construct(key, new ConfigurationSource() {});

                // Get that inner node from destination to continue the processing.
                InnerNode dstNode = dst.traverseChild(key, innerNodeVisitor());

                // "dstNode" is guaranteed to not be null even if "src" and "dst" match.
                // Null in "srcNode" means that we should initialize everything that we can in "dstNode"
                // unconditionally. It's only possible if we pass it as a source as well.
                addDefaults(srcNode == null ? dstNode : srcNode, dstNode);

                return null;
            }

            @Override public <N extends InnerNode> Object visitNamedListNode(String key, NamedListNode<N> srcNamedList) {
                // Here we don't need to preemptively initialise corresponsing field, because it can never be null.
                NamedListNode<?> dstNamedList = dst.traverseChild(key, namedListNodeVisitor());

                for (String namedListKey : srcNamedList.namedListKeys()) {
                    // But, in order to get non-null value from "dstNamedList.get(namedListKey)" we must explicitly
                    // ensure its existance.
                    dstNamedList.construct(namedListKey, new ConfigurationSource() {});

                    addDefaults(srcNamedList.get(namedListKey), dstNamedList.get(namedListKey));
                }

                return null;
            }
        });
    }

    /**
     * @return Visitor that returns leaf value or {@code null} if node is not a leaf.
     */
    public static ConfigurationVisitor<Serializable> leafNodeVisitor() {
        return new ConfigurationVisitor<>() {
            @Override public Serializable visitLeafNode(String key, Serializable val) {
                return val;
            }
        };
    }

    /**
     * @return Visitor that returns inner node or {@code null} if node is not an inner node.
     */
    public static ConfigurationVisitor<InnerNode> innerNodeVisitor() {
        return new ConfigurationVisitor<>() {
            @Override public InnerNode visitInnerNode(String key, InnerNode node) {
                return node;
            }
        };
    }

    /**
     * @return Visitor that returns named list node or {@code null} if node is not a named list node.
     */
    public static ConfigurationVisitor<NamedListNode<?>> namedListNodeVisitor() {
        return new ConfigurationVisitor<>() {
            @Override
            public <N extends InnerNode> NamedListNode<?> visitNamedListNode(String key, NamedListNode<N> node) {
                return node;
            }
        };
    }

    /** @see #patch(ConstructableTreeNode, TraversableTreeNode) */
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

    /** @see #patch(ConstructableTreeNode, TraversableTreeNode) */
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

            srcNode.traverseChildren(new ConfigurationVisitor<>() {
                @Override public Void visitLeafNode(String key, Serializable val) {
                    if (val != null)
                        dstNode.construct(key, new PatchLeafConfigurationSource(val));

                    return null;
                }

                @Override public Void visitInnerNode(String key, InnerNode node) {
                    if (node != null)
                        dstNode.construct(key, new PatchInnerConfigurationSource(node));

                    return null;
                }

                @Override public <N extends InnerNode> Void visitNamedListNode(String key, NamedListNode<N> node) {
                    if (node != null)
                        dstNode.construct(key, new PatchNamedListConfigurationSource(node));

                    return null;
                }
            });
        }
    }

    /** @see #patch(ConstructableTreeNode, TraversableTreeNode) */
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
