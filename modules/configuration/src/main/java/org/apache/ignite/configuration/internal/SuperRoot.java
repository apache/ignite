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

package org.apache.ignite.configuration.internal;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.tree.ConfigurationSource;
import org.apache.ignite.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.configuration.tree.InnerNode;

/** */
public final class SuperRoot extends InnerNode {
    /** */
    private final SortedMap<String, InnerNode> roots = new TreeMap<>();

    /** */
    private final Map<String, RootKey<?, ?>> allRootKeys;

    /** Copy constructor. */
    private SuperRoot(SuperRoot superRoot) {
        roots.putAll(superRoot.roots);

        allRootKeys = superRoot.allRootKeys;
    }

    /** */
    public SuperRoot(Map<String, RootKey<?, ?>> rootKeys) {
        allRootKeys = rootKeys;
    }

    /** */
    public SuperRoot(Map<String, RootKey<?, ?>> rootKeys, Map<RootKey<?, ?>, InnerNode> roots) {
        allRootKeys = rootKeys;

        for (Map.Entry<RootKey<?, ?>, InnerNode> entry : roots.entrySet())
            this.roots.put(entry.getKey().key(), entry.getValue());
    }

    /** */
    public void addRoot(RootKey<?, ?> rootKey, InnerNode root) {
        assert !roots.containsKey(rootKey.key()) : rootKey.key() + " : " + roots;
        assert allRootKeys.get(rootKey.key()) == rootKey : rootKey.key() + " : " + allRootKeys;

        roots.put(rootKey.key(), root);
    }

    /** */
    public void append(SuperRoot otherRoot) {
        //TODO IGNITE-14372 Revisit API of the super root.
        roots.putAll(otherRoot.roots);
    }

    /** */
    public InnerNode getRoot(RootKey<?, ?> rootKey) {
        return roots.get(rootKey.key());
    }

    /** {@inheritDoc} */
    @Override public <T> void traverseChildren(ConfigurationVisitor<T> visitor) {
        for (Map.Entry<String, InnerNode> entry : roots.entrySet())
            visitor.visitInnerNode(entry.getKey(), entry.getValue());
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
        assert src != null;

        RootKeyImpl<?, ?> rootKey = (RootKeyImpl<?, ?>)allRootKeys.get(key);

        if (rootKey == null)
            throw new NoSuchElementException(key);

        InnerNode root = roots.get(key);

        root = root == null ? rootKey.createRootNode() : root.copy();

        roots.put(key, root);

        src.descend(root);
    }

    /** {@inheritDoc} */
    @Override public boolean constructDefault(String key) throws NoSuchElementException {
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
