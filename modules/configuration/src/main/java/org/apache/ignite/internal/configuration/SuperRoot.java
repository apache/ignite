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

package org.apache.ignite.internal.configuration;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.jetbrains.annotations.Nullable;

/**
 * Holder of root configurations.
 */
public final class SuperRoot extends InnerNode {
    /** Root configurations. Mapping: {@link RootKey#key} -> configuration. */
    private final SortedMap<String, RootInnerNode> roots = new TreeMap<>();

    /** Function that creates root node by root name or returns {@code null} if root name is not found. */
    private final Function<String, RootInnerNode> nodeCreator;

    /**
     * Copy constructor.
     */
    private SuperRoot(SuperRoot superRoot) {
        roots.putAll(superRoot.roots);

        nodeCreator = superRoot.nodeCreator;
    }

    /**
     * Constructor.
     *
     * @param nodeCreator Function that creates root node by root name or returns {@code null} if root name is not found.
     */
    public SuperRoot(Function<String, RootInnerNode> nodeCreator) {
        this(nodeCreator, Map.of());
    }

    /**
     * Constructor.
     *
     * @param nodeCreator Function that creates root node by root name or returns {@code null} if root name is not found.
     * @param roots Map of roots belonging to this super root.
     */
    public SuperRoot(Function<String, RootInnerNode> nodeCreator, Map<RootKey<?, ?>, InnerNode> roots) {
        this.nodeCreator = nodeCreator;

        for (Map.Entry<RootKey<?, ?>, InnerNode> entry : roots.entrySet())
            this.roots.put(entry.getKey().key(), new RootInnerNode(entry.getKey(), entry.getValue().copy()));
    }

    /**
     * Adds a root to the super root.
     *
     * @param rootKey Root key.
     * @param root Root node.
     */
    public void addRoot(RootKey<?, ?> rootKey, InnerNode root) {
        assert !roots.containsKey(rootKey.key()) : rootKey.key() + " : " + roots;

        roots.put(rootKey.key(), new RootInnerNode(rootKey, root));
    }

    /**
     * Gets a root.
     *
     * @param rootKey Root key of the desired root.
     * @return Root node.
     */
    @Nullable public InnerNode getRoot(RootKey<?, ?> rootKey) {
        RootInnerNode root = roots.get(rootKey.key());

        return root == null ? null : root.node();
    }

    /** {@inheritDoc} */
    @Override public <T> void traverseChildren(ConfigurationVisitor<T> visitor, boolean includeInternal) {
        for (Map.Entry<String, RootInnerNode> e : roots.entrySet()) {
            if (includeInternal || !e.getValue().internal())
                visitor.visitInnerNode(e.getKey(), e.getValue().node());
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T traverseChild(
        String key,
        ConfigurationVisitor<T> visitor,
        boolean includeInternal
    ) throws NoSuchElementException {
        RootInnerNode root = roots.get(key);

        if (root == null || (!includeInternal && root.internal()))
            throw new NoSuchElementException(key);
        else
            return visitor.visitInnerNode(key, root.node());
    }

    /** {@inheritDoc} */
    @Override public void construct(
        String key,
        ConfigurationSource src,
        boolean includeInternal
    ) throws NoSuchElementException {
        RootInnerNode root = roots.get(key);

        if (root == null)
            root = nodeCreator.apply(key);

        if (root == null || !includeInternal && root.internal())
            throw new NoSuchElementException(key);

        if (src == null)
            roots.remove(key);
        else {
            roots.put(key, root = new RootInnerNode(root));

            src.descend(root.node());
        }
    }

    /** {@inheritDoc} */
    @Override public void constructDefault(String key) throws NoSuchElementException {
        throw new NoSuchElementException(key);
    }

    /** {@inheritDoc} */
    @Override public Class<?> schemaType() {
        return Object.class;
    }

    /** {@inheritDoc} */
    @Override public SuperRoot copy() {
        return new SuperRoot(this);
    }
}
