/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object for cache rebalance configuration properties.
 */
public class VisorCacheRebalanceConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache rebalance mode. */
    private CacheRebalanceMode mode;

    /** Cache rebalance batch size. */
    private int batchSize;

    /** Rebalance partitioned delay. */
    private long partitionedDelay;

    /** Time in milliseconds to wait between rebalance messages. */
    private long throttle;

    /** Rebalance timeout. */
    private long timeout;

    /** Rebalance batches prefetch count. */
    private long batchesPrefetchCnt;

    /** Cache rebalance order. */
    private int rebalanceOrder;

    /**
     * Default constructor.
     */
    public VisorCacheRebalanceConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for rebalance configuration properties.
     * @param ccfg Cache configuration.
     */
    public VisorCacheRebalanceConfiguration(CacheConfiguration ccfg) {
        mode = ccfg.getRebalanceMode();
        batchSize = ccfg.getRebalanceBatchSize();
        partitionedDelay = ccfg.getRebalanceDelay();
        throttle = ccfg.getRebalanceThrottle();
        timeout = ccfg.getRebalanceTimeout();
        batchesPrefetchCnt = ccfg.getRebalanceBatchesPrefetchCount();
        rebalanceOrder = ccfg.getRebalanceOrder();
    }

    /**
     * @return Cache rebalance mode.
     */
    public CacheRebalanceMode getMode() {
        return mode;
    }

    /**
     * @return Cache rebalance batch size.
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * @return Rebalance partitioned delay.
     */
    public long getPartitionedDelay() {
        return partitionedDelay;
    }

    /**
     * @return Time in milliseconds to wait between rebalance messages.
     */
    public long getThrottle() {
        return throttle;
    }

    /**
     * @return Rebalance timeout.
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * @return Batches count
     */
    public long getBatchesPrefetchCnt() {
        return batchesPrefetchCnt;
    }

    /**
     * @return Cache rebalance order.
     */
    public int getRebalanceOrder() {
        return rebalanceOrder;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeEnum(out, mode);
        out.writeInt(batchSize);
        out.writeLong(partitionedDelay);
        out.writeLong(throttle);
        out.writeLong(timeout);
        out.writeLong(batchesPrefetchCnt);
        out.writeInt(rebalanceOrder);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        mode = CacheRebalanceMode.fromOrdinal(in.readByte());
        batchSize = in.readInt();
        partitionedDelay = in.readLong();
        throttle = in.readLong();
        timeout = in.readLong();
        batchesPrefetchCnt = in.readLong();
        rebalanceOrder = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheRebalanceConfiguration.class, this);
    }
}
