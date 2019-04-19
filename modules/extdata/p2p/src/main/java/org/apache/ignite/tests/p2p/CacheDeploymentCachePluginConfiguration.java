/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.tests.p2p;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.plugin.CachePluginConfiguration;
import org.apache.ignite.plugin.CachePluginProvider;
import org.jetbrains.annotations.Nullable;

import javax.cache.Cache;

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
