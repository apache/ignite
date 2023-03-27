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

package org.apache.ignite.platform.plugin.cache;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.CachePluginConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Task to get a list of cache plugins.
 */
public class PlatformGetCachePluginsTask extends ComputeTaskAdapter<String, String[]> {
    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable String arg) {
        return Collections.singletonMap(new GetCachePluginsJob(arg), F.first(subgrid));
    }

    /** {@inheritDoc} */
    @Nullable @Override public String[] reduce(List<ComputeJobResult> results) {
        return results.get(0).getData();
    }

    /**
     * Job.
     */
    @SuppressWarnings("unchecked")
    private static class GetCachePluginsJob extends ComputeJobAdapter {
        /** */
        private final String cacheName;

        /** */
        @IgniteInstanceResource
        private final Ignite ignite;

        /** */
        GetCachePluginsJob(String cacheName) {
            this.cacheName = cacheName;
            ignite = null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public String[] execute() {
            CachePluginConfiguration[] cfg =
                    ignite.cache(cacheName).getConfiguration(CacheConfiguration.class).getPluginConfigurations();

            if (cfg == null)
                return null;

            String[] res = new String[cfg.length];

            for (int i = 0; i < cfg.length; i++)
                res[i] = cfg[i].getClass().getName();

            return res;
        }
    }
}
