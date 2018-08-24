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

package org.apache.ignite.client;

import java.util.Arrays;
import java.util.Objects;

import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.util.typedef.F;

/** */
public final class Comparers {
    /** Cannot instantiate. */
    private Comparers() {}

    /** */
    public static boolean equal(ClientConfiguration a, Object o) {
        if (!(o instanceof ClientConfiguration))
            return false;

        ClientConfiguration b = (ClientConfiguration)o;

        return Arrays.equals(a.getAddresses(), b.getAddresses()) &&
            a.isTcpNoDelay() == b.isTcpNoDelay() &&
            a.getTimeout() == b.getTimeout() &&
            a.getSendBufferSize() == b.getSendBufferSize() &&
            a.getReceiveBufferSize() == b.getReceiveBufferSize();
    }

    /**  */
    public static boolean equal(ClientCacheConfiguration a, Object o) {
        if (!(o instanceof ClientCacheConfiguration))
            return false;

        ClientCacheConfiguration b = (ClientCacheConfiguration)o;

        return Objects.equals(a.getName(), b.getName()) &&
            a.getAtomicityMode() == b.getAtomicityMode() &&
            a.getBackups() == b.getBackups() &&
            a.getCacheMode() == b.getCacheMode() &&
            a.isEagerTtl() == b.isEagerTtl() &&
            Objects.equals(a.getGroupName(), b.getGroupName()) &&
            a.getDefaultLockTimeout() == b.getDefaultLockTimeout() &&
            a.getPartitionLossPolicy() == b.getPartitionLossPolicy() &&
            a.isReadFromBackup() == b.isReadFromBackup() &&
            a.getRebalanceBatchSize() == b.getRebalanceBatchSize() &&
            a.getRebalanceBatchesPrefetchCount() == b.getRebalanceBatchesPrefetchCount() &&
            a.getRebalanceDelay() == b.getRebalanceDelay() &&
            a.getRebalanceMode() == b.getRebalanceMode() &&
            a.getRebalanceOrder() == b.getRebalanceOrder() &&
            a.getRebalanceThrottle() == b.getRebalanceThrottle() &&
            a.getWriteSynchronizationMode() == b.getWriteSynchronizationMode() &&
            a.isCopyOnRead() == b.isCopyOnRead() &&
            Objects.equals(a.getDataRegionName(), b.getDataRegionName()) &&
            a.isStatisticsEnabled() == b.isStatisticsEnabled() &&
            a.getMaxConcurrentAsyncOperations() == b.getMaxConcurrentAsyncOperations() &&
            a.getMaxQueryIteratorsCount() == b.getMaxQueryIteratorsCount() &&
            a.isOnheapCacheEnabled() == b.isOnheapCacheEnabled() &&
            a.getQueryDetailMetricsSize() == b.getQueryDetailMetricsSize() &&
            a.getQueryParallelism() == b.getQueryParallelism() &&
            a.isSqlEscapeAll() == b.isSqlEscapeAll() &&
            a.getSqlIndexMaxInlineSize() == b.getSqlIndexMaxInlineSize() &&
            Objects.equals(a.getSqlSchema(), b.getSqlSchema()) &&
            equalKeyConfiguration(a.getKeyConfiguration(), b.getKeyConfiguration()) &&
            Arrays.equals(a.getQueryEntities(), b.getQueryEntities());
    }

    /**
     * Check whether cache key configurations are equal..
     *
     * @param cfgs1 Config 1.
     * @param cfgs2 Config 2.
     * @return {@code True} if equal.
     */
    public static boolean equalKeyConfiguration(CacheKeyConfiguration[] cfgs1, CacheKeyConfiguration[] cfgs2) {
        if (cfgs1 == null && cfgs2 == null)
            return true;

        if (cfgs1 == null || cfgs2 == null)
            return false;

        if (cfgs1.length != cfgs2.length)
            return false;

        for (int i = 0; i < cfgs1.length; i++) {
            CacheKeyConfiguration cfg1 = cfgs1[i];
            CacheKeyConfiguration cfg2 = cfgs2[i];

            if (!F.eq(cfg1.getTypeName(), cfg2.getTypeName()) ||
                !F.eq(cfg1.getAffinityKeyFieldName(), cfg2.getAffinityKeyFieldName()))
                return false;
        }

        return true;
    }
}
