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

package org.apache.ignite.internal.processors.cache;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Map;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Information about configured caches sent from joining node.
 */
public class CacheJoinNodeDiscoveryData implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringInclude
    private final Map<String, CacheInfo> caches;

    /** */
    @GridToStringInclude
    private final Map<String, CacheInfo> templates;

    /** */
    @GridToStringInclude
    private final IgniteUuid cacheDeploymentId;

    /** */
    private final boolean startCaches;

    /**
     * @param cacheDeploymentId Deployment ID for started caches.
     * @param caches Caches.
     * @param templates Templates.
     * @param startCaches {@code True} if required to start all caches on joining node.
     */
    public CacheJoinNodeDiscoveryData(
        IgniteUuid cacheDeploymentId,
        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> caches,
        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> templates,
        boolean startCaches) {
        this.cacheDeploymentId = cacheDeploymentId;
        this.caches = caches;
        this.templates = templates;
        this.startCaches = startCaches;
    }

    /**
     * @return {@code True} if required to start all caches on joining node.
     */
    boolean startCaches() {
        return startCaches;
    }

    /**
     * @return Deployment ID assigned on joining node.
     */
    public IgniteUuid cacheDeploymentId() {
        return cacheDeploymentId;
    }

    /**
     * @return Templates configured on joining node.
     */
    public Map<String, CacheInfo> templates() {
        return templates;
    }

    /**
     * @return Caches configured on joining node.
     */
    public Map<String, CacheInfo> caches() {
        return caches;
    }

    /**
     *
     */
   public static class CacheInfo implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @GridToStringInclude
        private StoredCacheData cacheData;

        /** */
        @GridToStringInclude
        private CacheType cacheType;

        /** */
        @GridToStringInclude
        private boolean sql;

        /** Flags added for future usage. */
        private long flags;

        /** Statically configured flag */
        private boolean staticallyConfigured;

        /**
         * @param cacheData Cache data.
         * @param cacheType Cache type.
         * @param sql SQL flag - {@code true} if cache was created with {@code CREATE TABLE}.
         * @param flags Flags (for future usage).
         * @param staticallyConfigured {@code true} if it was configured by static config and {@code false} otherwise.
         */
        public CacheInfo(StoredCacheData cacheData, CacheType cacheType, boolean sql, long flags,
            boolean staticallyConfigured) {
            this.cacheData = cacheData;
            this.cacheType = cacheType;
            this.sql = sql;
            this.flags = flags;
            this.staticallyConfigured = staticallyConfigured;
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
         * @return SQL flag - {@code true} if cache was created with {@code CREATE TABLE}.
         */
        public boolean sql() {
            return sql;
        }

        /**
         * @return {@code true} if it was configured by static config and {@code false} otherwise.
         */
        public boolean isStaticallyConfigured() {
            return staticallyConfigured;
        }

        /**
         * @param ois ObjectInputStream.
         */
        private void readObject(ObjectInputStream ois)
            throws IOException, ClassNotFoundException {
            ObjectInputStream.GetField gf = ois.readFields();

            cacheData = (StoredCacheData)gf.get("cacheData", null);
            cacheType = (CacheType)gf.get("cacheType", null);
            sql = gf.get("sql", false);
            flags = gf.get("flags", 0L);
            staticallyConfigured = gf.get("staticallyConfigured", true);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CacheInfo.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheJoinNodeDiscoveryData.class, this);
    }
}
