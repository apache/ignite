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

package org.apache.ignite.platform;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlatformPlugin;
import org.apache.ignite.plugin.IgnitePlatformPluginTarget;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.UUID;

/**
 * Test plugin provider.
 */
public class PlatformTestPluginProvider implements PluginProvider<PluginConfiguration> {
    /** */
    private final PlatformTestPlugin plugin = new PlatformTestPlugin();

    /** {@inheritDoc} */
    @Override public String name() {
        return "PlatformTestPlugin";
    }

    /** {@inheritDoc} */
    @Override public String version() {
        return "1.2.3";
    }

    /** {@inheritDoc} */
    @Override public String copyright() {
        return "copyleft";
    }

    /** {@inheritDoc} */
    @Override public <T extends IgnitePlugin> T plugin() {
        return (T)plugin;
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T createComponent(PluginContext ctx, Class<T> cls) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void start(PluginContext ctx) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStart() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStop(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public Serializable provideDiscoveryData(UUID nodeId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void receiveDiscoveryData(UUID nodeId, Serializable data) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void validateNewNode(ClusterNode node) throws PluginValidationException {
        // No-op.
    }

    /**
     * Test plugin.
     */
    public static class PlatformTestPlugin implements IgnitePlatformPlugin {
        /** Plugin target. */
        private final IgnitePlatformPluginTarget target = new PlatformTestPluginTarget();

        /** {@inheritDoc} */
        @Override public IgnitePlatformPluginTarget platformTarget() {
            return target;
        }
    }

    /**
     * Test target.
     */
    public static class PlatformTestPluginTarget implements IgnitePlatformPluginTarget {
        /** */
        private static final int OP_READ_WRITE = 1;

        /** */
        private static final int OP_ERROR = 2;

        /** {@inheritDoc} */
        @Override public void invokeOperation(int opCode, BinaryRawReader reader, BinaryRawWriter writer) {
            switch (opCode) {
                case OP_READ_WRITE: {
                    Object data = reader.readObject();

                    writer.writeObject(data);

                    break;
                }

                case OP_ERROR: {
                    String text = reader.readString();

                    throw new IgniteException(text);
                }
            }
        }
    }
}
