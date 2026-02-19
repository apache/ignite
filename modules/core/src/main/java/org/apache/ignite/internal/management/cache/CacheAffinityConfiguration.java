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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.IgniteUtils.findNonPublicMethod;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;

/**
 * Data transfer object for affinity configuration properties.
 */
public class CacheAffinityConfiguration extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache affinity function. */
    @Order(0)
    String function;

    /** Cache affinity mapper. */
    @Order(1)
    String mapper;

    /** Number of backup nodes for one partition. */
    @Order(2)
    int partitionedBackups;

    /** Total partition count. */
    @Order(3)
    int partitions;

    /** Cache partitioned affinity exclude neighbors. */
    @Order(4)
    Boolean exclNeighbors;

    /**
     * Default constructor
     */
    public CacheAffinityConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for affinity configuration properties.
     *
     * @param ccfg Cache configuration.
     */
    public CacheAffinityConfiguration(CacheConfiguration ccfg) {
        AffinityFunction aff = ccfg.getAffinity();

        function = compactClass(aff);
        mapper = compactClass(ccfg.getAffinityMapper());
        partitions = aff.partitions();
        partitionedBackups = ccfg.getBackups();

        Method mthd = findNonPublicMethod(aff.getClass(), "isExcludeNeighbors");

        if (mthd != null) {
            try {
                exclNeighbors = (Boolean)mthd.invoke(aff);
            }
            catch (InvocationTargetException | IllegalAccessException ignored) {
                //  No-op.
            }
        }
    }

    /**
     * @return Cache affinity.
     */
    public String getFunction() {
        return function;
    }

    /**
     * @return Cache affinity mapper.
     */
    public String getMapper() {
        return mapper;
    }

    /**
     * @return Number of backup nodes for one partition.
     */
    public int getPartitionedBackups() {
        return partitionedBackups;
    }

    /**
     * @return Total partition count.
     */
    public int getPartitions() {
        return partitions;
    }

    /**
     * @return Cache partitioned affinity exclude neighbors.
     */
    @Nullable public Boolean isExcludeNeighbors() {
        return exclNeighbors;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheAffinityConfiguration.class, this);
    }
}
