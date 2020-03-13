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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;

/**
 * This exception represents a result of validation partitions for a cache group.
 * See {@link GridDhtPartitionsStateValidator#validatePartitionCountersAndSizes(GridDhtPartitionsExchangeFuture, GridDhtPartitionTopology, Map)}
 */
public class PartitionStateValidationException  extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Affinity topology version on which the validation was executed. */
    private final AffinityTopologyVersion topVer;

    /** Cache group id. */
    private final int grpId;

    /** Set of partitions that failed validation. */
    private final Set<Integer> parts;

    /**
     * Creates a new instance of PartitionStateValidationException.
     *
     * @param msg Error message.
     * @param topVer Topology version on which the validation was executed.
     * @param grpId Cache group id.
     * @param parts Set of partitions that failed validation.
     */
    public PartitionStateValidationException(
        String msg,
        AffinityTopologyVersion topVer,
        int grpId,
        Set<Integer> parts
    ) {
        super(msg);

        this.topVer = topVer;
        this.grpId = grpId;
        this.parts = parts;
    }

    /**
     * @return Topology version on which the validation was executed.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return cache group id.
     */
    public int groupId() {
        return grpId;
    }

    /**
     * @return Set of partitions that failed validation..
     */
    public Set<Integer> failedPartitions() {
        return parts;
    }
}
