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
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;

/** Visitor with just a single method to override. */
public abstract class AnyNodeConfigurationVisitor<T> implements ConfigurationVisitor<T> {
    /** {@inheritDoc} */
    @Override
    public final T visitLeafNode(String key, Serializable val) {
        return visitNode(key, val);
    }

    /** {@inheritDoc} */
    @Override
    public final T visitInnerNode(String key, InnerNode node) {
        return visitNode(key, node);
    }

    /** {@inheritDoc} */
    @Override
    public final T visitNamedListNode(String key, NamedListNode<?> node) {
        return visitNode(key, node);
    }

    /**
     * Visit tree node.
     *
     * @param key  Name of the node.
     * @param node {@link InnerNode}, {@link NamedListNode} or {@link Serializable} leaf.
     * @return Anything that implementation decides to return.
     */
    protected abstract T visitNode(String key, Object node);
}
