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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.configuration.NamedListChange;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;

import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.addDefaults;

/**
 * Configuration node implementation for the collection of named {@link InnerNode}s. Unlike implementations of
 * {@link InnerNode}, this class is used for every named list in configuration.
 *
 * @param <N> Type of the {@link InnerNode} that is stored in named list node object.
 */
public final class NamedListNode<N extends InnerNode> implements NamedListChange<N, N>, TraversableTreeNode, ConstructableTreeNode {
    /** Name of a synthetic configuration property that describes the order of elements in a named list. */
    public static final String ORDER_IDX = "<order>";

    /**
     * Name of a synthetic configuration property that's used to store "key" of the named list element in the storage.
     */
    public static final String NAME = "<name>";

    /** Configuration name for the synthetic key. */
    private final String syntheticKeyName;

    /** Supplier of new node objects when new list element node has to be created. */
    private final Supplier<N> valSupplier;

    /** Internal container for named list element. Maps keys to named list elements nodes with their internal ids. */
    private final OrderedMap<ElementDescriptor<N>> map;

    /** Mapping from internal ids to public keys. */
    private final Map<String, String> reverseIdMap;

    /**
     * Default constructor.
     *
     * @param syntheticKeyName Name of the synthetic configuration value that will represent keys in a specially ordered
     *      representation syntax.
     * @param valSupplier Closure to instantiate values.
     */
    public NamedListNode(String syntheticKeyName, Supplier<N> valSupplier) {
        this.syntheticKeyName = syntheticKeyName;
        this.valSupplier = valSupplier;
        map = new OrderedMap<>();
        reverseIdMap = new HashMap<>();
    }

    /**
     * Copy constructor.
     *
     * @param node Other node.
     */
    private NamedListNode(NamedListNode<N> node) {
        syntheticKeyName = node.syntheticKeyName;
        valSupplier = node.valSupplier;
        map = new OrderedMap<>();
        reverseIdMap = new HashMap<>(node.reverseIdMap);

        for (String key : node.map.keys())
            map.put(key, node.map.get(key).shallowCopy());
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(String key, ConfigurationVisitor<T> visitor) {
        return visitor.visitNamedListNode(key, this);
    }

    /** {@inheritDoc} */
    @Override public final List<String> namedListKeys() {
        return Collections.unmodifiableList(map.keys());
    }

    /** {@inheritDoc} */
    @Override public final N get(String key) {
        ElementDescriptor<N> element = map.get(key);

        return element == null ? null : element.value;
    }

    /** {@inheritDoc} */
    @Override public N get(int index) throws IndexOutOfBoundsException {
        ElementDescriptor<N> element = map.get(index);

        return element == null ? null : element.value;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return map.size();
    }

    /** {@inheritDoc} */
    @Override public NamedListChange<N, N> create(String key, Consumer<N> valConsumer) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(valConsumer, "valConsumer");

        checkNewKey(key);

        ElementDescriptor<N> element = newElementDescriptor();

        map.put(key, element);

        reverseIdMap.put(element.internalId, key);

        valConsumer.accept(element.value);

        return this;
    }

    /** {@inheritDoc} */
    @Override public NamedListChange<N, N> create(int index, String key, Consumer<N> valConsumer) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(valConsumer, "valConsumer");

        if (index < 0 || index > map.size())
            throw new IndexOutOfBoundsException(index);

        checkNewKey(key);

        ElementDescriptor<N> element = newElementDescriptor();

        map.putByIndex(index, key, element);

        reverseIdMap.put(element.internalId, key);

        valConsumer.accept(element.value);

        return this;
    }

    /** {@inheritDoc} */
    @Override public NamedListChange<N, N> createAfter(String precedingKey, String key, Consumer<N> valConsumer) {
        Objects.requireNonNull(precedingKey, "precedingKey");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(valConsumer, "valConsumer");

        if (!map.containsKey(precedingKey))
            throw new IllegalArgumentException("Element with name " + precedingKey + " doesn't exist.");

        checkNewKey(key);

        ElementDescriptor<N> element = newElementDescriptor();

        map.putAfter(precedingKey, key, element);

        reverseIdMap.put(element.internalId, key);

        valConsumer.accept(element.value);

        return this;
    }

    /** {@inheritDoc} */
    @Override public final NamedListChange<N, N> createOrUpdate(String key, Consumer<N> valConsumer) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(valConsumer, "valConsumer");

        if (map.containsKey(key) && map.get(key).value == null)
            throw new IllegalArgumentException("You can't create entity that has just been deleted [key=" + key + ']');

        ElementDescriptor<N> element = map.get(key);

        if (element == null) {
            element = newElementDescriptor();

            reverseIdMap.put(element.internalId, key);
        }
        else
            element = element.copy();

        map.put(key, element);

        valConsumer.accept(element.value);

        return this;
    }

    /** {@inheritDoc} */
    @Override public NamedListChange<N, N> rename(String oldKey, String newKey) {
        Objects.requireNonNull(oldKey, "oldKey");
        Objects.requireNonNull(newKey, "newKey");

        if (!map.containsKey(oldKey))
            throw new IllegalArgumentException("Element with name " + oldKey + " does not exist.");

        ElementDescriptor<N> element = map.get(oldKey);

        if (element.value == null) {
            throw new IllegalArgumentException(
                "Can't rename entity that has just been deleted [key=" + oldKey + ']'
            );
        }

        checkNewKey(newKey);

        map.rename(oldKey, newKey);

        reverseIdMap.put(element.internalId, newKey);

        return this;
    }

    /**
     * Checks that this new key can be inserted into the map.
     * @param key New key.
     * @throws IllegalArgumentException If key already exists.
     */
    private void checkNewKey(String key) {
        if (map.containsKey(key)) {
            if (map.get(key) == null)
                throw new IllegalArgumentException("You can't create entity that has just been deleted [key=" + key + ']');

            throw new IllegalArgumentException("Element with name " + key + " already exists.");
        }
    }

    /** {@inheritDoc} */
    @Override public NamedListChange<N, N> delete(String key) {
        Objects.requireNonNull(key, "key");

        if (map.containsKey(key))
            map.get(key).value = null;

        return this;
    }

    /**
     * @return Configuration name for the synthetic key.
     *
     * @see NamedConfigValue#syntheticKeyName()
     */
    public String syntheticKeyName() {
        return syntheticKeyName;
    }

    /**
     * Sets an internal id for the value associated with the passed key. Should not be used in arbitrary code. Refer
     * to {@link ConfigurationUtil#fillFromPrefixMap(ConstructableTreeNode, Map)} for further details on the usage.
     *
     * @param key Key to update. Should be present in the named list. Nothing will happen if the key is missing.
     * @param internalId New id to associate with the key.
     */
    public void setInternalId(String key, String internalId) {
        ElementDescriptor<N> element = map.get(key);

        if (element != null) {
            reverseIdMap.remove(element.internalId);

            element.internalId = internalId;

            reverseIdMap.put(internalId, key);
        }
    }

    /**
     * Returns internal id for the value associated with the passed key.
     *
     * @param key Key.
     * @return Internal id.
     *
     * @throws IllegalArgumentException If {@code key} is not found in the named list.
     */
    public String internalId(String key) {
        ElementDescriptor<N> element = map.get(key);

        if (element == null)
            throw new IllegalArgumentException("Element with name '" + key + "' does not exist.");

        return element.internalId;
    }

    /**
     * Returns public key associated with the internal id.
     *
     * @param internalId Internat id.
     * @return Key.
     */
    public String keyByInternalId(String internalId) {
        return reverseIdMap.get(internalId);
    }

    /**
     * Returns collection of internal ids in this named list node.
     *
     * @return Set of internal ids.
     */
    public Collection<String> internalIds() {
        return Collections.unmodifiableSet(reverseIdMap.keySet());
    }

    /**
     * Deletes named list element.
     *
     * @param key Element's key.
     */
    public void forceDelete(String key) {
        ElementDescriptor<N> removed = map.remove(key);

        if (removed != null)
            reverseIdMap.remove(removed.internalId);
    }

    /**
     * Reorders keys in the map.
     *
     * @param orderedKeys List of keys in new order. Must have the same set of keys in it.
     */
    public void reorderKeys(List<String> orderedKeys) {
        map.reorderKeys(orderedKeys);
    }

    /** {@inheritDoc} */
    @Override public void construct(String key, ConfigurationSource src, boolean includeInternal) {
        if (src == null)
            delete(key);
        else
            createOrUpdate(key, src::descend);
    }

    /** {@inheritDoc} */
    @Override public NamedListNode<N> copy() {
        return new NamedListNode<>(this);
    }

    /**
     * Creates new element instance with initialized defaults.
     *
     * @return New element instance with initialized defaults.
     */
    private NamedListNode.ElementDescriptor<N> newElementDescriptor() {
        N newElement = valSupplier.get();

        addDefaults(newElement);

        return new ElementDescriptor<>(newElement);
    }

    /**
     * Descriptor for internal named list element representation. Has node itself and its internal id.
     *
     * @param <N> Type of the node.
     */
    private static class ElementDescriptor<N extends InnerNode> {
        /** Element's internal id. */
        public String internalId;

        /** Element node value. */
        public N value;

        /**
         * Constructor.
         *
         * @param value Node instance.
         */
        ElementDescriptor(N value) {
            this.value = value;
            // Remove dashes so that id would be a bit shorter and easier to validate in tests.
            // This string won't be visible by end users anyway.
            internalId = UUID.randomUUID().toString().replace("-", "");
        }

        /**
         * Private constructor with entire fields list.
         *
         * @param internalId Internal id.
         * @param value Node instance.
         */
        private ElementDescriptor(String internalId, N value) {
            this.internalId = internalId;
            this.value = value;
        }

        /**
         * Makes a copy of the element descriptor. Not to be confused with {@link #shallowCopy()}.
         *
         * @return New instance with the same internal id but copied node instance.
         * @see InnerNode#copy()
         */
        public ElementDescriptor<N> copy() {
            return new ElementDescriptor<>(internalId, (N)value.copy());
        }

        /**
         * Makes a copy of the element descriptor, preserving same fields values.
         *
         * @return New instance with the same internal id and node instance.
         */
        public ElementDescriptor<N> shallowCopy() {
            return new ElementDescriptor<>(internalId, value);
        }
    }
}
