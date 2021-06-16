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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.InnerNode;

/** */
public final class SuperRoot extends InnerNode {
    /** */
    private final SortedMap<String, InnerNode> roots = new TreeMap<>();

    /** */
    private final Function<String, InnerNode> nodeCreator;

    /** Copy constructor. */
    private SuperRoot(SuperRoot superRoot) {
        roots.putAll(superRoot.roots);

        nodeCreator = superRoot.nodeCreator;
    }

    /**
     * @param nodeCreator Function that creates root node by root name or returns {@code null} if root name is not found
     */
    public SuperRoot(Function<String, InnerNode> nodeCreator) {
        this.nodeCreator = nodeCreator;
    }

    /**
     * @param nodeCreator Function that creates root node by root name or returns {@code null} if root name is not found
     * @param roots Map of roots belonging to this super root.
     */
    public SuperRoot(Function<String, InnerNode> nodeCreator, Map<RootKey<?, ?>, InnerNode> roots) {
        this.nodeCreator = nodeCreator;

        for (Map.Entry<RootKey<?, ?>, InnerNode> entry : roots.entrySet())
            this.roots.put(entry.getKey().key(), entry.getValue().copy());
    }

    /**
     * @param nodeCreator Function that creates root node by root name or returns {@code null} if root name is not found
     * @param superRoots List of super roots to merge into even more superior root.
     */
    public SuperRoot(Function<String, InnerNode> nodeCreator, List<SuperRoot> superRoots) {
        this(nodeCreator);

        for (SuperRoot superRoot : superRoots)
            roots.putAll(superRoot.roots);
    }

    /**
     * Adds a root to the super root.
     * @param rootKey Root key.
     * @param root Root node.
     */
    public void addRoot(RootKey<?, ?> rootKey, InnerNode root) {
        assert !roots.containsKey(rootKey.key()) : rootKey.key() + " : " + roots;

        roots.put(rootKey.key(), root);
    }

    /**
     * Gets a root.
     * @param rootKey Root key of the desired root.
     * @return Root node.
     */
    public InnerNode getRoot(RootKey<?, ?> rootKey) {
        return roots.get(rootKey.key());
    }

    /** {@inheritDoc} */
    @Override public <T> void traverseChildren(ConfigurationVisitor<T> visitor) {
        for (String key : new LinkedHashSet<>(roots.keySet()))
            visitor.visitInnerNode(key, roots.get(key));
    }

    /** {@inheritDoc} */
    @Override public <T> T traverseChild(String key, ConfigurationVisitor<T> visitor) throws NoSuchElementException {
        InnerNode root = roots.get(key);

        if (root == null)
            throw new NoSuchElementException(key);

        return visitor.visitInnerNode(key, root);
    }

    /** {@inheritDoc} */
    @Override public void construct(String key, ConfigurationSource src) throws NoSuchElementException {
        if (src == null) {
            roots.remove(key);

            return;
        }

        InnerNode root = roots.get(key);

        if (root == null) {
            root = nodeCreator.apply(key);

            if (root == null)
                throw new NoSuchElementException(key);
        }
        else
            root = root.copy();

        roots.put(key, root);

        src.descend(root);
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
