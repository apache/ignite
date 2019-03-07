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
package org.apache.ignite.internal.sql.optimizer.affinity;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Client context. Passed to partition resolver on thin clients.
 */
public class PartitionClientContext {

    /** Number of partitions. */
    private int parts;

    /** Mask to use in calculation when partitions count is power of 2. */
    private int mask = -1;

    /**
     * Resolve partition.
     *
     * @param arg Argument.
     * @param typ Type.
     * @param cacheName Cache name.
     * @return Partition or {@code null} if cannot be resolved.
     */
    // TODO: 06.03.19 very-very primitive implementation. Ignore it if affinity function is not randevous or custom mapper is set, etc.
    @Nullable public Integer partition(Object arg, @Nullable PartitionParameterType typ, String cacheName) {
        // TODO: 06.03.19 check what will happen if typ is not actually one of PartitionParameterType instances.
        // TODO: 06.03.19 exception handling?
        Object key = PartitionDataTypeUtils.convert(arg, typ);

        assert cacheName != null;

        if (key instanceof KeyCacheObject) {
            int part = ((KeyCacheObject)key).partition();

            if (part >= 0)
                return part;
        }

        assert cacheName != null;

        // TODO: 06.03.19 on server we actually retrieve affinityKey from key. return aff.affFunc.partition(aff.affinityKey(key));
        if (mask >= 0) {
            int h;
            // TODO: 07.03.19 seems that we actually know part count so we can use mask in appropriate way.
            return ((h = key.hashCode()) ^ (h >>> 16)) & mask;
        }

        return U.safeAbs(key.hashCode() % parts);
    }

    // TODO: 06.03.19 Do we need this in order to support custom number of partitions.
    /**
     * Sets total number of partitions.If the number of partitions is a power of two,
     * the PowerOfTwo hashing method will be used.  Otherwise the Standard hashing
     * method will be applied.
     *
     * @param parts Total number of partitions.
     * @return {@code this} for chaining.
     */
    public void setPartitions(int parts) {
        A.ensure(parts <= CacheConfiguration.MAX_PARTITIONS_COUNT,
            "parts <= " + CacheConfiguration.MAX_PARTITIONS_COUNT);
        A.ensure(parts > 0, "parts > 0");

        this.parts = parts;

        mask = (parts & (parts - 1)) == 0 ? parts - 1 : -1;
    }
}
