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

package org.apache.ignite.internal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.plugin.*;
import org.apache.ignite.spi.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 *
 */
public class GridPluginComponent implements GridComponent {
    /** */
    private final PluginProvider plugin;

    /**
     * @param plugin Plugin provider.
     */
    public GridPluginComponent(PluginProvider plugin) {
        this.plugin = plugin;
    }

    /**
     * @return Plugin instance.
     */
    public PluginProvider plugin() {
        return plugin;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void start() throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        plugin.stop(cancel);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        plugin.onIgniteStart();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        plugin.onIgniteStop(cancel);
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryDataExchangeType discoveryDataType() {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object collectDiscoveryData(UUID nodeId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void onDiscoveryDataReceived(Object data) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteSpiNodeValidationResult validateNode(ClusterNode node) {
        try {
            plugin.validateNewNode(node);

            return null;
        }
        catch (PluginValidationException e) {
            return new IgniteSpiNodeValidationResult(e.nodeId(), e.getMessage(), e.remoteMessage());
        }
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        // No-op.
    }
}
