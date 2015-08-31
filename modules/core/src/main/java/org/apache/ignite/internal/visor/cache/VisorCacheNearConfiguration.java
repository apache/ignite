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

import java.io.Serializable;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.evictionPolicyMaxSize;

/**
 * Data transfer object for near cache configuration properties.
 */
public class VisorCacheNearConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Flag indicating if near cache enabled. */
    private boolean nearEnabled;

    /** Near cache start size. */
    private int nearStartSize;

    /** Near cache eviction policy. */
    private String nearEvictPlc;

    /** Near cache eviction policy maximum size. */
    private Integer nearEvictMaxSize;

    /**
     * @param ccfg Cache configuration.
     * @return Data transfer object for near cache configuration properties.
     */
    public static VisorCacheNearConfiguration from(CacheConfiguration ccfg) {
        VisorCacheNearConfiguration cfg = new VisorCacheNearConfiguration();

        cfg.nearEnabled = GridCacheUtils.isNearEnabled(ccfg);

        if (cfg.nearEnabled) {
            NearCacheConfiguration nccfg = ccfg.getNearConfiguration();

            cfg.nearStartSize = nccfg.getNearStartSize();
            cfg.nearEvictPlc = compactClass(nccfg.getNearEvictionPolicy());
            cfg.nearEvictMaxSize = evictionPolicyMaxSize(nccfg.getNearEvictionPolicy());
        }

        return cfg;
    }

    /**
     * @return {@code true} if near cache enabled.
     */
    public boolean nearEnabled() {
        return nearEnabled;
    }

    /**
     * @return Near cache start size.
     */
    public int nearStartSize() {
        return nearStartSize;
    }

    /**
     * @return Near cache eviction policy.
     */
    @Nullable public String nearEvictPolicy() {
        return nearEvictPlc;
    }

    /**
     * @return Near cache eviction policy max size.
     */
    @Nullable public Integer nearEvictMaxSize() {
        return nearEvictMaxSize;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheNearConfiguration.class, this);
    }
}