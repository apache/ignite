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

package org.apache.ignite.internal.visor.cache;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Job that collect cache metrics from node.
 */
public class VisorCacheConfigurationCollectorJob
    extends VisorJob<Collection<IgniteUuid>, Map<IgniteUuid, VisorCacheConfiguration>> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Create job with given argument.
     *
     * @param arg Whether to collect metrics for all caches or for specified cache name only.
     * @param debug Debug flag.
     */
    public VisorCacheConfigurationCollectorJob(Collection<IgniteUuid> arg, boolean debug) {
        super(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected Map<IgniteUuid, VisorCacheConfiguration> run(Collection<IgniteUuid> arg) {
        Collection<IgniteCacheProxy<?, ?>> caches = ignite.context().cache().jcaches();

        boolean all = arg == null || arg.isEmpty();

        Map<IgniteUuid, VisorCacheConfiguration> res = U.newHashMap(caches.size());

        for (IgniteCacheProxy<?, ?> cache : caches) {
            IgniteUuid deploymentId = cache.context().dynamicDeploymentId();

            if (all || arg.contains(deploymentId))
                res.put(deploymentId, config(cache.getConfiguration(CacheConfiguration.class)));
        }

        return res;
    }

    /**
     * @param ccfg Cache configuration.
     * @return Data transfer object to send it to Visor.
     */
    protected VisorCacheConfiguration config(CacheConfiguration ccfg) {
        return new VisorCacheConfiguration().from(ignite, ccfg);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheConfigurationCollectorJob.class, this);
    }
}