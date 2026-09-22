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

import javax.cache.configuration.Factory;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.evictionPolicyMaxSize;

/**
 * Data transfer object for near cache configuration properties.
 */
public class CacheNearConfiguration extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Flag indicating if near cache enabled. */
    @Order(0)
    boolean nearEnabled;

    /** Near cache start size. */
    @Order(1)
    int nearStartSize;

    /** Near cache eviction policy. */
    @Order(2)
    String nearEvictPlc;

    /** Near cache eviction policy maximum size. */
    @Order(3)
    Integer nearEvictMaxSize;

    /**
     * Default constructor.
     */
    public CacheNearConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for near cache configuration properties.
     *
     * @param ccfg Cache configuration.
     */
    public CacheNearConfiguration(CacheConfiguration ccfg) {
        nearEnabled = GridCacheUtils.isNearEnabled(ccfg);

        if (nearEnabled) {
            NearCacheConfiguration nccfg = ccfg.getNearConfiguration();

            final Factory nearEvictionPlc = nccfg.getNearEvictionPolicyFactory();

            nearStartSize = nccfg.getNearStartSize();
            nearEvictPlc = compactClass(nearEvictionPlc);
            nearEvictMaxSize = evictionPolicyMaxSize(nearEvictionPlc);
        }
    }

    /**
     * @return {@code true} if near cache enabled.
     */
    public boolean isNearEnabled() {
        return nearEnabled;
    }

    /**
     * @return Near cache start size.
     */
    public int getNearStartSize() {
        return nearStartSize;
    }

    /**
     * @return Near cache eviction policy.
     */
    @Nullable public String getNearEvictPolicy() {
        return nearEvictPlc;
    }

    /**
     * @return Near cache eviction policy max size.
     */
    @Nullable public Integer getNearEvictMaxSize() {
        return nearEvictMaxSize;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheNearConfiguration.class, this);
    }
}
