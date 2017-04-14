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
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.LessNamingBean;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.evictionPolicyMaxSize;

/**
 * Data transfer object for eviction configuration properties.
 */
public class VisorCacheEvictionConfiguration implements Serializable, LessNamingBean {
    /** */
    private static final long serialVersionUID = 0L;

    /** Eviction policy. */
    private String plc;

    /** Cache eviction policy max size. */
    private Integer plcMaxSize;

    /** Eviction filter to specify which entries should not be evicted. */
    private String filter;

    /**
     * @param ccfg Cache configuration.
     * @return Data transfer object for eviction configuration properties.
     */
    public static VisorCacheEvictionConfiguration from(CacheConfiguration ccfg) {
        VisorCacheEvictionConfiguration cfg = new VisorCacheEvictionConfiguration();

        final EvictionPolicy plc = ccfg.getEvictionPolicy();

        cfg.plc = compactClass(plc);
        cfg.plcMaxSize = evictionPolicyMaxSize(plc);
        cfg.filter = compactClass(ccfg.getEvictionFilter());

        return cfg;
    }

    /**
     * @return Eviction policy.
     */
    @Nullable public String policy() {
        return plc;
    }

    /**
     * @return Cache eviction policy max size.
     */
    @Nullable public Integer policyMaxSize() {
        return plcMaxSize;
    }

    /**
     * @return Eviction filter to specify which entries should not be evicted.
     */
    @Nullable public String filter() {
        return filter;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheEvictionConfiguration.class, this);
    }
}
