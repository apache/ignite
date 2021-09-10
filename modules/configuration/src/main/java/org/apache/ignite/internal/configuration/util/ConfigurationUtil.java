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
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.RandomAccess;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InternalConfiguration;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.apache.ignite.internal.configuration.tree.TraversableTreeNode;

import static java.util.stream.Collectors.toList;

/** */
public class ConfigurationUtil {
    /** Configuration source that copies values without modifying tham. */
    static final ConfigurationSource EMPTY_CFG_SRC = new ConfigurationSource() {};

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
     * @param includeInternal Include internal configuration nodes (private configuration extensions).
     * @return Either {@link TraversableTreeNode} or {@link Serializable} depending on the keys and schema.
     * @throws KeyNotFoundException If node is not found.
     */
    public static Object find(
        List<String> keys,
        TraversableTreeNode node,
        boolean includeInternal
    ) throws KeyNotFoundException {
        assert keys instanceof RandomAccess : keys.getClass();

        var visitor = new ConfigurationVisitor<>() {
            /** Current index of the key in the {@code keys}. */
            private int i;

            /** {@inheritDoc} */
            @Override public Object visitLeafNode(String key, Serializable val) {
                if (i != keys.size())
                    throw new KeyNotFoundException("Configuration value '" + join(keys.subList(0, i)) + "' is a leaf");
                else
                    return val;
            }

            /** {@inheritDoc} */
            @Override public Object visitInnerNode(String key, InnerNode node) {
                if (i == keys.size())
                    return node;
                else if (node == null)
                    throw new KeyNotFoundException("Configuration node '" + join(keys.subList(0, i)) + "' is null");
                else {
                    try {
                        return node.traverseChild(keys.get(i++), this, includeInternal);
                    }
                    catch (NoSuchElementException e) {
                        throw new KeyNotFoundException(
                            "Configuration '" + join(keys.subList(0, i)) + "' is not found"
                        );
                    }
                }
            }

            /** {@inheritDoc} */
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
                if (node instanceof NamedListNode) {
                    descendToNamedListNode((NamedListNode<?>)node);

                    return;
                }

                for (Map.Entry<String, ?> entry : map.entrySet()) {
                    String key = entry.getKey();
                    Object val = entry.getValue();

                    assert val == null || val instanceof Map || val instanceof Serializable;

                    // Ordering of indexes must be skipped here because they make no sense in this context.
                    if (key.equals(NamedListNode.ORDER_IDX) || key.equals(NamedListNode.NAME))
                        continue;

                    if (val == null)
                        node.construct(key, null, true);
                    else if (val instanceof Map)
                        node.construct(key, new InnerConfigurationSource((Map<String, ?>)val), true);
                    else {
                        assert val instanceof Serializable;

                        node.construct(key, new LeafConfigurationSource((Serializable)val), true);
                    }
                }
            }

            /**
             * Specific implementation of {@link #descend(ConstructableTreeNode)} that descends into named list node and
             * sets a proper ordering to named list elements.
             *
             * @param node Named list node under construction.
             */
            private void descendToNamedListNode(NamedListNode<?> node) {
                // This list must be mutable and RandomAccess.
                var orderedKeys = new ArrayList<>(((NamedListView<?>)node).namedListKeys());

                for (Map.Entry<String, ?> entry : map.entrySet()) {
                    String internalId = entry.getKey();
                    Object val = entry.getValue();

                    assert val == null || val instanceof Map || val instanceof Serializable;

                    String oldKey = node.keyByInternalId(internalId);

                    if (val == null) {
                        // Given that this particular method is applied to modify existing trees rather than
                        // creating new trees, a "hack" is required in this place. "construct" is designed to create
                        // "change" objects, thus it would just nullify named list element instead of deleting it.
                        node.forceDelete(oldKey);
                    }
                    else if (val instanceof Map) {
                        Map<String, ?> map = (Map<String, ?>)val;
                        int sizeDiff = 0;

                        // For every named list entry modification we must take its index into account.
                        // We do this by modifying "orderedKeys" when index is explicitly passed.
                        Object idxObj = map.get(NamedListNode.ORDER_IDX);

                        if (idxObj != null)
                            sizeDiff++;

                        String newKey = (String)map.get(NamedListNode.NAME);

                        if (newKey != null)
                            sizeDiff++;

                        boolean construct = map.size() != sizeDiff;

                        if (oldKey == null) {
                            node.construct(newKey, new InnerConfigurationSource(map), true);

                            node.setInternalId(newKey, internalId);
                        }
                        else if (newKey != null) {
                            node.rename(oldKey, newKey);

                            if (construct)
                                node.construct(newKey, new InnerConfigurationSource(map), true);
                        }
                        else if (construct)
                            node.construct(oldKey, new InnerConfigurationSource(map), true);
                        // Else it's just index adjustment after new elements insertion.

                        if (newKey == null)
                            newKey = oldKey;

                        if (idxObj != null) {
                            assert idxObj instanceof Integer : val;

                            int idx = (Integer)idxObj;

                            if (idx >= orderedKeys.size()) {
                                // Updates can come in arbitrary order. This means that array may be too small
                                // during batch creation. In this case we have to insert enough nulls before
                                // invoking "add" method for actual key.
                                orderedKeys.ensureCapacity(idx + 1);

                                while (idx != orderedKeys.size())
                                    orderedKeys.add(null);

                                orderedKeys.add(newKey);
                            }
                            else
                                orderedKeys.set(idx, newKey);
                        }
                    }
                    else {
                        assert val instanceof Serializable;

                        node.construct(oldKey, new LeafConfigurationSource((Serializable)val), true);
                    }
                }

                node.reorderKeys(orderedKeys.size() > node.size()
                    ? orderedKeys.subList(0, node.size())
                    : orderedKeys
                );
            }
        }

        var src = new InnerConfigurationSource(prefixMap);

        src.descend(node);
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
     * Fill {@code node} node with default values where nodes are {@code null}.
     *
     * @param node Node.
     */
    public static void addDefaults(InnerNode node) {
        node.traverseChildren(new ConfigurationVisitor<>() {
            /** {@inheritDoc} */
            @Override public Object visitLeafNode(String key, Serializable val) {
                // If source value is null then inititalise the same value on the destination node.
                if (val == null)
                    node.constructDefault(key);

                return null;
            }

            /** {@inheritDoc} */
            @Override public Object visitInnerNode(String key, InnerNode innerNode) {
                // Instantiate field in destination node before doing something else or copy it if it wasn't null.
                node.construct(key, EMPTY_CFG_SRC, true);

                addDefaults(node.traverseChild(key, innerNodeVisitor(), true));

                return null;
            }

            /** {@inheritDoc} */
            @Override public <N extends InnerNode> Object visitNamedListNode(String key, NamedListNode<N> namedList) {
                // Copy internal map.
                node.construct(key, EMPTY_CFG_SRC, true);

                namedList = (NamedListNode<N>)node.traverseChild(key, namedListNodeVisitor(), true);

                for (String namedListKey : namedList.namedListKeys()) {
                    if (namedList.get(namedListKey) != null) {
                        // Copy the element.
                        namedList.construct(namedListKey, EMPTY_CFG_SRC, true);

                        addDefaults(namedList.get(namedListKey));
                    }
                }

                return null;
            }
        }, true);
    }

    /**
     * Recursively removes all nullified named list elements.
     *
     * @param node Inner node for processing.
     */
    public static void dropNulls(InnerNode node) {
        node.traverseChildren(new ConfigurationVisitor<>() {
            @Override public Object visitInnerNode(String key, InnerNode innerNode) {
                dropNulls(innerNode);

                return null;
            }

            @Override public <N extends InnerNode> Object visitNamedListNode(String key, NamedListNode<N> namedList) {
                for (String namedListKey : namedList.namedListKeys()) {
                    N element = namedList.get(namedListKey);

                    if (element == null)
                        namedList.forceDelete(namedListKey);
                    else
                        dropNulls(element);
                }

                return null;
            }
        }, true);
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
            /** {@inheritDoc} */
            @Override public <N extends InnerNode> NamedListNode<?> visitNamedListNode(String key, NamedListNode<N> node) {
                return node;
            }
        };
    }

    /**
     * Checks that the configuration type of root keys is equal to the storage type.
     *
     * @throws IllegalArgumentException If the configuration type of the root keys is not equal to the storage type.
     */
    public static void checkConfigurationType(Collection<RootKey<?, ?>> rootKeys, ConfigurationStorage storage) {
        for (RootKey<?, ?> key : rootKeys) {
            if (key.type() != storage.type()) {
                throw new IllegalArgumentException("Invalid root key configuration type [key=" + key +
                    ", storage=" + storage.getClass().getName() + ", storageType=" + storage.type() + "]");
            }
        }
    }

    /**
     * Get and check schemas and their extensions.
     *
     * @param extensions Schema extensions with {@link InternalConfiguration}.
     * @return Internal schema extensions. Mapping: original of the scheme -> internal extensions.
     * @throws IllegalArgumentException If the schema or its extensions are not valid.
     */
    public static Map<Class<?>, Set<Class<?>>> internalSchemaExtensions(Collection<Class<?>> extensions) {
        if (extensions.isEmpty())
            return Map.of();
        else {
            Map<Class<?>, Set<Class<?>>> res = new HashMap<>();

            for (Class<?> extension : extensions) {
                if (!extension.isAnnotationPresent(InternalConfiguration.class)) {
                    throw new IllegalArgumentException(String.format(
                        "Extension should contain @%s: %s",
                        InternalConfiguration.class.getSimpleName(),
                        extension.getName()
                    ));
                }
                else
                    res.computeIfAbsent(extension.getSuperclass(), cls -> new HashSet<>()).add(extension);
            }

            return res;
        }
    }

    /**
     * Checks whether configuration schema field represents primitive configuration value.
     *
     * @param schemaField Configuration Schema class field.
     * @return {@code true} if field represents primitive configuration.
     */
    public static boolean isValue(Field schemaField) {
        return schemaField.isAnnotationPresent(Value.class);
    }

    /**
     * Checks whether configuration schema field represents regular configuration value.
     *
     * @param schemaField Configuration Schema class field.
     * @return {@code true} if field represents regular configuration.
     */
    public static boolean isConfigValue(Field schemaField) {
        return schemaField.isAnnotationPresent(ConfigValue.class);
    }

    /**
     * Checks whether configuration schema field represents named list configuration value.
     *
     * @param schemaField Configuration Schema class field.
     * @return {@code true} if field represents named list configuration.
     */
    public static boolean isNamedConfigValue(Field schemaField) {
        return schemaField.isAnnotationPresent(NamedConfigValue.class);
    }

    /**
     * Get the value of a {@link NamedConfigValue#syntheticKeyName}.
     *
     * @param field Configuration Schema class field.
     * @return Name for the synthetic key.
     */
    public static String syntheticKeyName(Field field) {
        assert isNamedConfigValue(field) : field;

        return field.getAnnotation(NamedConfigValue.class).syntheticKeyName();
    }

    /**
     * Get the value of a {@link Value#hasDefault}.
     *
     * @param field Configuration Schema class field.
     * @return Indicates that the current configuration value has a default value.
     */
    public static boolean hasDefault(Field field) {
        assert isValue(field) : field;

        return field.getAnnotation(Value.class).hasDefault();
    }

    /**
     * Get the default value of a {@link Value}.
     *
     * @param field Configuration Schema class field.
     * @return Default value.
     */
    public static Object defaultValue(Field field) {
        assert hasDefault(field) : field;

        try {
            Object o = field.getDeclaringClass().getDeclaredConstructor().newInstance();

            field.setAccessible(true);

            return field.get(o);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get merged schema extension fields.
     *
     * @param extensions Configuration schema extensions ({@link InternalConfiguration}).
     * @return Unique fields of the schema and its extensions.
     * @throws IllegalArgumentException If there is a conflict in field names.
     */
    public static Set<Field> extensionsFields(Collection<Class<?>> extensions) {
        if (extensions.isEmpty())
            return Set.of();
        else {
            Map<String, Field> res = new HashMap<>();

            for (Class<?> extension : extensions) {
                assert extension.isAnnotationPresent(InternalConfiguration.class) : extension;

                for (Field field : extension.getDeclaredFields()) {
                    String fieldName = field.getName();

                    if (res.containsKey(fieldName)) {
                        throw new IllegalArgumentException(String.format(
                            "Duplicate field names are not allowed [field=%s, classes=%s]",
                            field,
                            classNames(res.get(fieldName), field)
                        ));
                    }
                    else
                        res.put(fieldName, field);
                }
            }

            return Set.copyOf(res.values());
        }
    }

    /**
     * Collect all configuration schemes with {@link ConfigurationRoot} or {@link Config}
     * including all sub configuration schemes for fields with {@link ConfigValue} or {@link NamedConfigValue}.
     *
     * @param schemaClasses Configuration schemas (starting points) with {@link ConfigurationRoot} or {@link Config}.
     * @return All configuration schemes with {@link ConfigurationRoot} or {@link Config}.
     * @throws IllegalArgumentException If the configuration schemas does not contain
     *      {@link ConfigurationRoot} or {@link Config}.
     */
    public static Set<Class<?>> collectSchemas(Collection<Class<?>> schemaClasses) {
        if (schemaClasses.isEmpty())
            return Set.of();
        else {
            Set<Class<?>> res = new HashSet<>();

            Queue<Class<?>> queue = new ArrayDeque<>(Set.copyOf(schemaClasses));

            while (!queue.isEmpty()) {
                Class<?> cls = queue.poll();

                if (!cls.isAnnotationPresent(ConfigurationRoot.class) && !cls.isAnnotationPresent(Config.class)) {
                    throw new IllegalArgumentException(String.format(
                        "Configuration schema must contain @%s or @%s: %s",
                        ConfigurationRoot.class.getSimpleName(),
                        Config.class.getSimpleName(),
                        cls.getName()
                    ));
                }
                else {
                    res.add(cls);

                    for (Field f : cls.getDeclaredFields()) {
                        if ((f.isAnnotationPresent(ConfigValue.class) || f.isAnnotationPresent(NamedConfigValue.class))
                            && !res.contains(f.getType()))
                            queue.add(f.getType());
                    }
                }
            }

            return res;
        }
    }

    /**
     * Get the class names of the fields.
     *
     * @param fields Fields.
     * @return Fields class names.
     */
    public static List<String> classNames(Field... fields) {
        return Stream.of(fields).map(Field::getDeclaringClass).map(Class::getName).collect(toList());
    }
}
