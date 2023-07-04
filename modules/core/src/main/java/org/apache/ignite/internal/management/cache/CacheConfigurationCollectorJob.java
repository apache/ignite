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

package org.apache.ignite.internal.management.cache;

import java.util.Collection;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.util.VisorTaskUtils;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Job that collect cache configuration from node.
 */
public class CacheConfigurationCollectorJob
    extends VisorJob<CacheConfigurationCollectorTaskArg, Map<String, CacheConfiguration>> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Create job with given argument.
     *
     * @param arg Whether to collect metrics for all caches or for specified cache name only or by regex.
     * @param debug Debug flag.
     */
    public CacheConfigurationCollectorJob(CacheConfigurationCollectorTaskArg arg, boolean debug) {
        super(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected Map<String, CacheConfiguration> run(CacheConfigurationCollectorTaskArg arg) {
        Collection<IgniteCacheProxy<?, ?>> caches = ignite.context().cache().jcaches();

        Pattern ptrn = arg.getRegex() != null ? Pattern.compile(arg.getRegex()) : null;

        boolean all = F.isEmpty(arg.getCacheNames());

        boolean hasPtrn = ptrn != null;

        Map<String, CacheConfiguration> res = U.newHashMap(caches.size());

        for (IgniteCacheProxy<?, ?> cache : caches) {
            if (!cache.context().userCache())
                continue;

            String cacheName = cache.getName();

            boolean matched = hasPtrn ? ptrn.matcher(cacheName).find() : all || arg.getCacheNames().contains(cacheName);

            if (!VisorTaskUtils.isRestartingCache(ignite, cacheName) && matched) {
                CacheConfiguration cfg =
                    config(cache.getConfiguration(org.apache.ignite.configuration.CacheConfiguration.class),
                        cache.context().dynamicDeploymentId());

                res.put(cacheName, cfg);
            }
        }

        return res;
    }

    /**
     * @param ccfg Cache configuration.
     * @return Data transfer object to send it to Visor.
     */
    protected CacheConfiguration config(org.apache.ignite.configuration.CacheConfiguration ccfg, IgniteUuid dynamicDeploymentId) {
        return new CacheConfiguration(ignite, ccfg, dynamicDeploymentId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheConfigurationCollectorJob.class, this);
    }
}
