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
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.RandomAccess;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.ConfigurationWrongPolymorphicTypeIdException;
import org.apache.ignite.configuration.DirectConfigurationProperty;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.DirectAccess;
import org.apache.ignite.configuration.annotation.InternalConfiguration;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.apache.ignite.internal.configuration.tree.TraversableTreeNode;
import org.jetbrains.annotations.Nullable;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/** */
public class ConfigurationUtil {
    /** Configuration source that copies values without modifying tham. */
    public static final ConfigurationSource EMPTY_CFG_SRC = new ConfigurationSource() {};

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
     * @throws WrongPolymorphicTypeIdException If the type of the polymorphic configuration instance is not correct (unknown).
     */
    public static <T> T find(
        List<String> keys,
        TraversableTreeNode node,
        boolean includeInternal
    ) throws KeyNotFoundException, WrongPolymorphicTypeIdException {
        assert keys instanceof RandomAccess : keys.getClass();

        var visitor = new ConfigurationVisitor<T>() {
            /** Current index of the key in the {@code keys}. */
            private int i;

            /** {@inheritDoc} */
            @Override public T visitLeafNode(String key, Serializable val) {
                if (i != keys.size())
                    throw new KeyNotFoundException("Configuration value '" + join(keys.subList(0, i)) + "' is a leaf");
                else
                    return (T)val;
            }

            /** {@inheritDoc} */
            @Override public T visitInnerNode(String key, InnerNode node) {
                if (i == keys.size())
                    return (T)node;
                else if (node == null)
                    throw new KeyNotFoundException("Configuration node '" + join(keys.subList(0, i)) + "' is null");
                else {
                    try {
                        return node.traverseChild(keys.get(i++), this, includeInternal);
                    }
                    catch (NoSuchElementException e) {
                        throw new KeyNotFoundException(
                            "Configuration value '" + join(keys.subList(0, i)) + "' has not been found"
                        );
                    }
                    catch (ConfigurationWrongPolymorphicTypeIdException e) {
                        throw new WrongPolymorphicTypeIdException(
                            "Polymorphic configuration type is not correct: " + e.getMessage(),
                            e
                        );
                    }
                }
            }

            /** {@inheritDoc} */
            @Override public T visitNamedListNode(String key, NamedListNode<?> node) {
                if (i == keys.size())
                    return (T)node;
                else {
                    String name = keys.get(i++);

                    return visitInnerNode(name, node.getInnerNode(name));
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
    public static Map<String, ?> toPrefixMap(Map<String, ? extends Serializable> rawConfig) {
        Map<String, Object> res = new HashMap<>();

        for (Map.Entry<String, ? extends Serializable> entry : rawConfig.entrySet()) {
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
    public static void fillFromPrefixMap(InnerNode node, Map<String, ?> prefixMap) {
        new InnerConfigurationSource(prefixMap).descend(node);
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
                // If source value is null then initialise the same value on the destination node.
                if (val == null)
                    node.constructDefault(key);

                return null;
            }

            /** {@inheritDoc} */
            @Override public Object visitInnerNode(String key, InnerNode innerNode) {
                InnerNode childNode = node.traverseChild(key, innerNodeVisitor(), true);

                // Instantiate field in destination node before doing something else.
                if (childNode == null) {
                    node.construct(key, EMPTY_CFG_SRC, true);

                    childNode = node.traverseChild(key, innerNodeVisitor(), true);
                }

                addDefaults(childNode);

                return null;
            }

            /** {@inheritDoc} */
            @Override public Object visitNamedListNode(String key, NamedListNode<?> namedList) {
                namedList = node.traverseChild(key, namedListNodeVisitor(), true);

                for (String namedListKey : namedList.namedListKeys()) {
                    if (namedList.getInnerNode(namedListKey) != null) {
                        // Copy the element.
                        namedList.construct(namedListKey, EMPTY_CFG_SRC, true);

                        addDefaults(namedList.getInnerNode(namedListKey));
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

            @Override public Object visitNamedListNode(String key, NamedListNode<?> namedList) {
                for (String namedListKey : namedList.namedListKeys()) {
                    InnerNode element = namedList.getInnerNode(namedListKey);

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
            @Override public NamedListNode<?> visitNamedListNode(String key, NamedListNode<?> node) {
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
     * Collect all configuration schemas with {@link ConfigurationRoot}, {@link Config} or {@link PolymorphicConfig}
     * including all sub configuration schemas for fields with {@link ConfigValue} or {@link NamedConfigValue}.
     *
     * @param schemaClasses Configuration schemas (starting points) with {@link ConfigurationRoot}, {@link Config}
     *      or {@link PolymorphicConfig}.
     * @return All configuration schemas with {@link ConfigurationRoot}, {@link Config}, or {@link PolymorphicConfig}.
     * @throws IllegalArgumentException If the configuration schemas does not contain
     *      {@link ConfigurationRoot}, {@link Config} or {@link PolymorphicConfig}.
     */
    public static Set<Class<?>> collectSchemas(Collection<Class<?>> schemaClasses) {
        if (schemaClasses.isEmpty())
            return Set.of();

        Set<Class<?>> res = new HashSet<>();

        Queue<Class<?>> queue = new ArrayDeque<>(Set.copyOf(schemaClasses));

        while (!queue.isEmpty()) {
            Class<?> cls = queue.poll();

            if (!cls.isAnnotationPresent(ConfigurationRoot.class) && !cls.isAnnotationPresent(Config.class) &&
                !cls.isAnnotationPresent(PolymorphicConfig.class)) {
                throw new IllegalArgumentException(String.format(
                    "Configuration schema must contain @%s or @%s or @%s: %s",
                    ConfigurationRoot.class.getSimpleName(),
                    Config.class.getSimpleName(),
                    PolymorphicConfig.class.getSimpleName(),
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

    /**
     * Get the class names of the fields.
     *
     * @param fields Fields.
     * @return Fields class names.
     */
    public static List<String> classNames(Field... fields) {
        return Stream.of(fields).map(Field::getDeclaringClass).map(Class::getName).collect(toList());
    }

    /**
     * Extracts the "direct" value from the given property.
     *
     * @param property Property to get the value from.
     * @return "direct" value of the property.
     * @throws ClassCastException if the property has not been annotated with {@link DirectAccess}.
     *
     * @see DirectAccess
     * @see DirectConfigurationProperty
     */
    public static <T> T directValue(ConfigurationProperty<T> property) {
        return ((DirectConfigurationProperty<T>)property).directValue();
    }

    /**
     * Get configuration schemas and their validated internal extensions.
     *
     * @param extensions Schema extensions with {@link InternalConfiguration}.
     * @return Mapping: original of the scheme -> internal schema extensions.
     * @throws IllegalArgumentException If the schema extension is invalid.
     * @see InternalConfiguration
     */
    public static Map<Class<?>, Set<Class<?>>> internalSchemaExtensions(Collection<Class<?>> extensions) {
        return schemaExtensions(extensions, InternalConfiguration.class);
    }

    /**
     * Get polymorphic extensions of configuration schemas.
     *
     * @param extensions Schema extensions with {@link PolymorphicConfigInstance}.
     * @return Mapping: polymorphic scheme -> extensions (instances) of polymorphic configuration.
     * @throws IllegalArgumentException If the schema extension is invalid.
     * @see PolymorphicConfig
     * @see PolymorphicConfigInstance
     */
    public static Map<Class<?>, Set<Class<?>>> polymorphicSchemaExtensions(Collection<Class<?>> extensions) {
        return schemaExtensions(extensions, PolymorphicConfigInstance.class);
    }

    /**
     * Get configuration schemas and their validated extensions.
     * Configuration schema is the parent class of the extension.
     *
     * @param extensions Schema extensions.
     * @param annotationClass Annotation class that the extension should have.
     * @return Mapping: original of the scheme -> schema extensions.
     * @throws IllegalArgumentException If the schema extension is invalid.
     */
    private static Map<Class<?>, Set<Class<?>>> schemaExtensions(
        Collection<Class<?>> extensions,
        Class<? extends Annotation> annotationClass
    ) {
        if (extensions.isEmpty())
            return Map.of();

        Map<Class<?>, Set<Class<?>>> res = new HashMap<>();

        for (Class<?> extension : extensions) {
            if (!extension.isAnnotationPresent(annotationClass)) {
                throw new IllegalArgumentException(String.format(
                    "Extension should contain @%s: %s",
                    annotationClass.getSimpleName(),
                    extension.getName()
                ));
            }
            else
                res.computeIfAbsent(extension.getSuperclass(), cls -> new HashSet<>()).add(extension);
        }

        return res;
    }

    /**
     * Collects fields of configuration schema that contain {@link Value}, {@link ConfigValue},
     *      {@link NamedConfigValue} or {@link PolymorphicId}.
     *
     * @param schemaClass Configuration schema class.
     * @return Schema fields.
     */
    public static List<Field> schemaFields(Class<?> schemaClass) {
        return Arrays.stream(schemaClass.getDeclaredFields())
            .filter(f -> isValue(f) || isConfigValue(f) || isNamedConfigValue(f) || isPolymorphicId(f))
            .collect(toList());
    }

    /**
     * Collects fields of configuration schema extensions that contain {@link Value}, {@link ConfigValue},
     *      {@link NamedConfigValue} or {@link PolymorphicId}.
     *
     * @param extensions Configuration schema extensions.
     * @param uniqueByName Checking the uniqueness of fields by {@link Field#getName name}.
     * @return Schema extensions fields.
     * @throws IllegalArgumentException If there was a field name conflict for {@code uniqueByName == true}.
     */
    public static Collection<Field> extensionsFields(Collection<Class<?>> extensions, boolean uniqueByName) {
        if (extensions.isEmpty())
            return List.of();

        if (uniqueByName) {
            return extensions.stream()
                .flatMap(cls -> Arrays.stream(cls.getDeclaredFields()))
                .filter(f -> isValue(f) || isConfigValue(f) || isNamedConfigValue(f) || isPolymorphicId(f))
                .collect(toMap(
                    Field::getName,
                    identity(),
                    (f1, f2) -> {
                        throw new IllegalArgumentException(String.format(
                            "Duplicate field names are not allowed [field=%s, classes=%s]",
                            f1.getName(),
                            classNames(f1, f2)
                        ));
                    },
                    LinkedHashMap::new
                )).values();
        }
        else {
            return extensions.stream()
                .flatMap(cls -> Arrays.stream(cls.getDeclaredFields()))
                .filter(f -> isValue(f) || isConfigValue(f) || isNamedConfigValue(f) || isPolymorphicId(f))
                .collect(toList());
        }
    }

    /**
     * Checks whether configuration schema field contains {@link PolymorphicId}.
     *
     * @param schemaField Configuration schema class field.
     * @return {@code true} if the field contains {@link PolymorphicId}.
     */
    public static boolean isPolymorphicId(Field schemaField) {
        return schemaField.isAnnotationPresent(PolymorphicId.class);
    }

    /**
     * Checks whether configuration schema contains {@link PolymorphicConfig}.
     *
     * @param schemaClass Configuration schema class.
     * @return {@code true} if the schema contains {@link PolymorphicConfig}.
     */
    public static boolean isPolymorphicConfig(Class<?> schemaClass) {
        return schemaClass.isAnnotationPresent(PolymorphicConfig.class);
    }

    /**
     * Checks whether configuration schema contains {@link PolymorphicConfigInstance}.
     *
     * @param schemaClass Configuration schema class.
     * @return {@code true} if the schema contains {@link PolymorphicConfigInstance}.
     */
    public static boolean isPolymorphicConfigInstance(Class<?> schemaClass) {
        return schemaClass.isAnnotationPresent(PolymorphicConfigInstance.class);
    }

    /**
     * Returns the identifier of the polymorphic configuration.
     *
     * @param schemaClass Configuration schema class.
     * @return Identifier of the polymorphic configuration.
     * @see PolymorphicConfigInstance#value
     */
    public static String polymorphicInstanceId(Class<?> schemaClass) {
        assert isPolymorphicConfigInstance(schemaClass) : schemaClass.getName();

        return schemaClass.getAnnotation(PolymorphicConfigInstance.class).value();
    }

    /**
     * Prepares a map for further work with it:
     * 1)If a deleted element of the named list is encountered, then this subtree becomes {@code null};
     * 2)If a {@code null} leaf is encountered due to a change in the polymorphic configuration, then remove it.
     *
     * @param prefixMap Prefix map, constructed from the storage notification data or its subtree.
     */
    public static void compressDeletedEntries(Map<String, ?> prefixMap) {
        for (Iterator<? extends Map.Entry<String, ?>> it = prefixMap.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, ?> entry = it.next();

            Object value = entry.getValue();

            if (value instanceof Map) {
                Map<?, ?> map = (Map<?, ?>)value;

                // If an element of the named list is removed then {@link NamedListNode#NAME}
                // will be {@code null} and the entire subtree can be replaced with {@code null}.
                if (map.containsKey(NamedListNode.NAME) && map.get(NamedListNode.NAME) == null)
                    entry.setValue(null);
            }
            else if (value == null) {
                // If there was a change in the type of polymorphic configuration,
                // then the fields of the old configuration will be {@code null}, so we can get rid of them.
                it.remove();
            }
        }

        // Continue recursively.
        for (Object value : prefixMap.values()) {
            if (value instanceof Map)
                compressDeletedEntries((Map<String, ?>)value);
        }
    }

    /**
     * Leaf configuration source.
     */
    public static class LeafConfigurationSource implements ConfigurationSource {
        /** Value. */
        private final Serializable val;

        /**
         * Constructor.
         *
         * @param val Value.
         */
        public LeafConfigurationSource(Serializable val) {
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

    /**
     * Inner configuration source.
     */
    private static class InnerConfigurationSource implements ConfigurationSource {
        /** Prefix map. */
        private final Map<String, ?> map;

        /**
         * Constructor.
         *
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
                else
                    node.construct(key, new LeafConfigurationSource((Serializable)val), true);
            }
        }

        /** {@inheritDoc} */
        @Override public @Nullable String polymorphicTypeId(String fieldName) {
            return (String)map.get(fieldName);
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
                else
                    node.construct(oldKey, new LeafConfigurationSource((Serializable)val), true);
            }

            node.reorderKeys(orderedKeys.size() > node.size()
                ? orderedKeys.subList(0, node.size())
                : orderedKeys
            );
        }
    }
}
