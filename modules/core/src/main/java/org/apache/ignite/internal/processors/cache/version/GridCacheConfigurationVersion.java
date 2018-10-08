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
package org.apache.ignite.internal.processors.cache.version;

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Grid cache configuration version. Used for correct merge cache configuration in moment joining of node. Make sense
 * only for dynamically created user caches.
 */
public class GridCacheConfigurationVersion implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Version number. */
    private volatile int id;

    /** Reason for last update version number. */
    private volatile GridCacheConfigurationChangeAction lastAct;

    /** Cache name. */
    private final String cacheName;

    /** Cache group name. */
    private final String cacheGrpName;

    /** {@code true} if cache was statically configured. */
    private final boolean staticallyConfigured;

    /**
     * Creates initial version.
     *
     * @param cacheName Cache name.
     * @param cacheGrpName Cache group name.
     * @param staticallyConfigured Statically configured cache flag.
     */
    public GridCacheConfigurationVersion(String cacheName, String cacheGrpName, boolean staticallyConfigured) {
        this.cacheName = cacheName;
        this.cacheGrpName = cacheGrpName;
        this.staticallyConfigured = staticallyConfigured;
    }

     /**
     * Updates version.
     *
     * @param action Reason for update version.
     */
    public void updateVersion(@NotNull GridCacheConfigurationChangeAction action) {
        synchronized (this) {
            if (isNeedUpdateVersion(action)) {
                this.id++;

                this.lastAct = action;
            }
        }
    }

    /**
     * Checks, that version must be updated.
     *
     * @param act Reason for update version.
     * @return {@code true} if version must be updated.
     */
    public boolean isNeedUpdateVersion(@NotNull GridCacheConfigurationChangeAction act) {
        if (staticallyConfigured)
            return false;

        if (act == GridCacheConfigurationChangeAction.META_CHANGED)
            return true;

        return lastAct != act;
    }

    /**
     * @return Version number.
     */
    public int id() { return id; }

    /**
     * @return Reason for last update version number.
     */
    public GridCacheConfigurationChangeAction lastAction() { return lastAct; }

    /**
     * @return Cache group name.
     */
    public String cacheGroupName() { return cacheGrpName; }

    /**
     * @return Cache name.
     */
    public String cacheName() { return cacheName; }

    /**
     * @return {@code true} if cache was statically configured.
     */
    public boolean staticallyConfigured() { return staticallyConfigured; }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheConfigurationVersion.class, this);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        GridCacheConfigurationVersion ver = (GridCacheConfigurationVersion)o;
        return id == ver.id &&
            staticallyConfigured == ver.staticallyConfigured &&
            lastAct == ver.lastAct &&
            Objects.equals(cacheName, ver.cacheName) &&
            Objects.equals(cacheGrpName, ver.cacheGrpName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(cacheName, cacheGrpName, staticallyConfigured);
    }
}