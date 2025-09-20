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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.jetbrains.annotations.Nullable;

/**
 * Supply message with supplier error transfer support.
 */
public class GridDhtPartitionSupplyErrorMessage extends GridDhtPartitionSupplyMessage {
    /** Supplying process error. */
    @Order(value = 10, method = "error")
    private Throwable err;

    /**
     * Default constructor.
     */
    public GridDhtPartitionSupplyErrorMessage() {
        // No-op.
    }

    /**
     * @param rebalanceId Rebalance id.
     * @param grpId Group id.
     * @param topVer Topology version.
     * @param addDepInfo Add dep info.
     * @param err Supply process error.
     */
    public GridDhtPartitionSupplyErrorMessage(
        long rebalanceId,
        int grpId,
        AffinityTopologyVersion topVer,
        boolean addDepInfo,
        Throwable err
    ) {
        super(rebalanceId, grpId, topVer, addDepInfo);

        this.err = err;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Throwable error() {
        return err;
    }

    /**
     * @param err New supplying process error.
     */
    public void error(Throwable err) {
        this.err = err;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 158;
    }
}
