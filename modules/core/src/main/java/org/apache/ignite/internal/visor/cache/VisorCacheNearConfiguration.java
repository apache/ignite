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

import org.apache.ignite.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.*;

/**
 * Data transfer object for near cache configuration properties.
 */
public class VisorCacheNearConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Flag to enable/disable near cache eviction policy. */
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

        cfg.nearEnabled(GridCacheUtils.isNearEnabled(ccfg));
        cfg.nearStartSize(ccfg.getNearStartSize());
        cfg.nearEvictPolicy(compactClass(ccfg.getNearEvictionPolicy()));
        cfg.nearEvictMaxSize(evictionPolicyMaxSize(ccfg.getNearEvictionPolicy()));

        return cfg;
    }

    /**
     * @return Flag to enable/disable near cache eviction policy.
     */
    public boolean nearEnabled() {
        return nearEnabled;
    }

    /**
     * @param nearEnabled New flag to enable/disable near cache eviction policy.
     */
    public void nearEnabled(boolean nearEnabled) {
        this.nearEnabled = nearEnabled;
    }

    /**
     * @return Near cache start size.
     */
    public int nearStartSize() {
        return nearStartSize;
    }

    /**
     * @param nearStartSize New near cache start size.
     */
    public void nearStartSize(int nearStartSize) {
        this.nearStartSize = nearStartSize;
    }

    /**
     * @return Near cache eviction policy.
     */
    @Nullable public String nearEvictPolicy() {
        return nearEvictPlc;
    }

    /**
     * @param nearEvictPlc New near cache eviction policy.
     */
    public void nearEvictPolicy(String nearEvictPlc) {
        this.nearEvictPlc = nearEvictPlc;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheNearConfiguration.class, this);
    }

    /**
     * @return Near cache eviction policy max size.
     */
    @Nullable public Integer nearEvictMaxSize() {
        return nearEvictMaxSize;
    }

    /**
     * @param nearEvictMaxSize New near cache eviction policy max size.
     */
    public void nearEvictMaxSize(@Nullable Integer nearEvictMaxSize) {
        this.nearEvictMaxSize = nearEvictMaxSize;
    }
}
