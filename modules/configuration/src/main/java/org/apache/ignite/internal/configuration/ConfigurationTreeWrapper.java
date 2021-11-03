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

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.configuration.ConfigurationTree;
import org.apache.ignite.configuration.notifications.ConfigurationListener;

/**
 * {@link ConfigurationTree} wrapper.
 *
 * @param <VIEWT> Value type of the node.
 * @param <CHANGET> Type of the object that changes this node's value.
 */
public class ConfigurationTreeWrapper<VIEWT, CHANGET> implements ConfigurationTree<VIEWT, CHANGET> {
    /** Configuration tree. */
    protected final ConfigurationTree<VIEWT, CHANGET> configTree;

    /**
     * Constructor.
     *
     * @param configTree Configuration tree.
     */
    public ConfigurationTreeWrapper(ConfigurationTree<VIEWT, CHANGET> configTree) {
        this.configTree = configTree;
    }

    /** {@inheritDoc} */
    @Override public String key() {
        return configTree.key();
    }

    /** {@inheritDoc} */
    @Override public VIEWT value() {
        return configTree.value();
    }

    /** {@inheritDoc} */
    @Override public void listen(ConfigurationListener<VIEWT> listener) {
        configTree.listen(listener);
    }

    /** {@inheritDoc} */
    @Override public void stopListen(ConfigurationListener<VIEWT> listener) {
        configTree.stopListen(listener);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> change(Consumer<CHANGET> change) {
        return configTree.change(change);
    }
}
