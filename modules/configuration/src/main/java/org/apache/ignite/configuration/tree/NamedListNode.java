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

package org.apache.ignite.configuration.tree;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** */
public final class NamedListNode<N extends InnerNode> implements NamedListView<N>, NamedListChange<N>, TraversableTreeNode, Cloneable {
    /** */
    private final Supplier<N> valSupplier;

    /** */
    private final Map<String, N> map;

    /**
     * Default constructor.
     *
     * @param valSupplier Closure to instantiate values.
     */
    public NamedListNode(Supplier<N> valSupplier) {
        this.valSupplier = valSupplier;
        map = new HashMap<>();
    }

    /**
     * Copy constructor.
     *
     * @param node Other node.
     */
    private NamedListNode(NamedListNode<N> node) {
        valSupplier = node.valSupplier;
        map = new HashMap<>(node.map);
    }

    /** {@inheritDoc} */
    @Override public void accept(String key, ConfigurationVisitor visitor) {
        visitor.visitNamedListNode(key, this);
    }

    /** {@inheritDoc} */
    @Override public final Set<String> namedListKeys() {
        return Collections.unmodifiableSet(map.keySet());
    }

    /** {@inheritDoc} */
    @Override public final N get(String key) {
        return map.get(key);
    }

    /** {@inheritDoc} */
    @Override public final NamedListChange<N> put(String key, Consumer<N> valConsumer) {
        Objects.requireNonNull(valConsumer, "valConsumer");

        if (map.containsKey(key) && map.get(key) == null)
            throw new IllegalStateException("You can't add entity that has just been deleted [key=" + key + ']');

        N val = map.get(key);

        if (val == null)
            map.put(key, val = valSupplier.get());

        valConsumer.accept(val);

        return this;
    }

    /** {@inheritDoc} */
    @Override public NamedListChange<N> remove(String key) {
        if (map.containsKey(key) && map.get(key) != null)
            throw new IllegalStateException("You can't add entity that has just been modified [key=" + key + ']');

        map.put(key, null);

        return this;
    }

    /** */
    public void delete(String key) {
        map.remove(key);
    }

    /** {@inheritDoc} */
    @Override public Object clone() {
        return new NamedListNode<>(this);
    }
}
