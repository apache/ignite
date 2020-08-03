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

package org.apache.ignite.tests.p2p;

import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.plugin.CachePluginConfiguration;
import org.apache.ignite.plugin.CachePluginProvider;
import org.jetbrains.annotations.Nullable;

/**
 * Test cache plugin configuration for cache deployment tests.
 */
public class CacheDeploymentCachePluginConfiguration<K, V> implements CachePluginConfiguration<K, V> {
    private static class CacheDeploymentCachePluginProvider implements CachePluginProvider {
        /** {@inheritDoc} */
        @Nullable @Override public Object createComponent(Class cls) {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object unwrapCacheEntry(final Cache.Entry mutableEntry, final Class cls) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void start() throws IgniteCheckedException {

        }

        /** {@inheritDoc} */
        @Override public void stop(boolean cancel) {

        }

        /** {@inheritDoc} */
        @Override public void onIgniteStart() throws IgniteCheckedException {

        }

        /** {@inheritDoc} */
        @Override public void onIgniteStop(boolean cancel) {

        }

        /** {@inheritDoc} */
        @Override public void validate() throws IgniteCheckedException {

        }

        /** {@inheritDoc} */
        @Override public void validateRemote(CacheConfiguration locCfg, CacheConfiguration rmtCfg,
            ClusterNode rmtNode) throws IgniteCheckedException {

        }
    }
}
