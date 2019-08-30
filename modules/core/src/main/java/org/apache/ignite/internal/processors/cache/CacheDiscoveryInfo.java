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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Information about cache configured on a node send by discovery during join/reconnect.
 */
public class CacheDiscoveryInfo implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Flag indicates that cache configuration was get from persistence directory rather than configured statically. */
    public static final long FLAG_PERSISTED_CONFIGURATION = 1L;

    /** Flag indicates that cache is statically configured on a node. */
    public static final long FLAG_STATICALLY_CONFIGURED = 1L << 1;

    /** Flag indicates that cache is created using SQL. */
    public static final long FLAG_SQL = 1L << 2;

    /** Flag indicates that cache is template. */
    public static final long FLAG_TEMPLATE = 1L << 3;

    /** Flag indicates that cache is near. */
    public static final long FLAG_NEAR = 1L << 4;

    /**
     * Deployment id.
     */
    private final IgniteUuid deploymentId;

    /**
     *
     */
    @GridToStringInclude
    private final StoredCacheData cacheData;

    /**
     *
     */
    @GridToStringInclude
    private final CacheType cacheType;

    /**
     * Flags representing various options of cache info.
     */
    private long flags;

    /**
     * @param deploymentId         Deployment id.
     * @param cacheData            Cache data.
     * @param cacheType            Cache type.
     * @param flags                Flags (for future usage).
     */
    public CacheDiscoveryInfo(
        IgniteUuid deploymentId,
        StoredCacheData cacheData,
        CacheType cacheType,
        long... flags
    ) {
        this.deploymentId = deploymentId;
        this.cacheData = cacheData;
        this.cacheType = cacheType;
        this.flags = Arrays.stream(flags).reduce((allFlags, flag) -> allFlags |= flag).orElse(0);
    }

    /**
     * @return Deployment id.
     */
    public IgniteUuid deploymentId() {
        return deploymentId;
    }

    /**
     * @return Cache data.
     */
    public StoredCacheData cacheData() {
        return cacheData;
    }

    /**
     * @return Cache type.
     */
    public CacheType cacheType() {
        return cacheType;
    }

    /**
     * @return {@code true} if cache configuration was get from persistence directory rather than configured statically.
     */
    public boolean isConfigurationPersisted() {
        return hasFlag(FLAG_PERSISTED_CONFIGURATION);
    }

    /**
     * @return {@code true} if it was configured by static config and {@code false} otherwise.
     */
    public boolean isStaticallyConfigured() {
        return hasFlag(FLAG_STATICALLY_CONFIGURED);
    }

    /**
     * @return SQL flag - {@code true} if cache was created with {@code CREATE TABLE}.
     */
    public boolean isCreatedUsingSql() {
        return hasFlag(FLAG_SQL);
    }

    /**
     * @return {@code true} if this is cache template.
     */
    public boolean isTemplate() {
        return hasFlag(FLAG_TEMPLATE);
    }

    /**
     * @return {@code true} if has near configuration enabled.
     */
    public boolean isNear() {
        return hasFlag(FLAG_NEAR);
    }

    /**
     * @param flag Flag.
     * @return {@code true} if flag is presented.
     */
    public boolean hasFlag(long flag) {
        return (flags & flag) == flag;
    }

    /**
     * @return Flags.
     */
    public long flags() {
        return flags;
    }

    /**
     * {@inheritDoc}
     */
    @Override public String toString() {
        return S.toString(CacheDiscoveryInfo.class, this);
    }
}
