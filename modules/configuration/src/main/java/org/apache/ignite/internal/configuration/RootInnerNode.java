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

import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.InternalConfiguration;
import org.apache.ignite.internal.configuration.tree.InnerNode;

/**
 * Holder of root configuration.
 */
public class RootInnerNode {
    /** Root node. */
    private final InnerNode node;

    /** Internal configuration. */
    private final boolean internal;

    /**
     * Constructor.
     *
     * @param node Root node.
     * @param key Root key.
     */
    public RootInnerNode(RootKey<?, ?> key, InnerNode node) {
        this.node = node;

        internal = key.internal();
    }

    /**
     * Copy constructor.
     */
    public RootInnerNode(RootInnerNode rootInnerNode) {
        node = rootInnerNode.node.copy();
        internal = rootInnerNode.internal;
    }

    /**
     * Get root node.
     *
     * @return Root node.
     */
    public InnerNode node() {
        return node;
    }

    /**
     * Check if the root configuration is marked with {@link InternalConfiguration}.
     *
     * @return {@code true} if the root configuration is internal.
     */
    public boolean internal() {
        return internal;
    }
}
