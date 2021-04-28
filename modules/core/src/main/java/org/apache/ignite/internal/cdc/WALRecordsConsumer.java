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

package org.apache.ignite.internal.cdc;

import java.util.EnumSet;
import java.util.Iterator;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cdc.CaptureDataChangeConsumer;
import org.apache.ignite.cdc.ChangeEvent;
import org.apache.ignite.cdc.ChangeEventOrder;
import org.apache.ignite.cdc.ChangeEventType;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.UnwrappedDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.CREATE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;

/**
 * Transform {@link DataEntry} to {@link ChangeEvent} and sends it to {@link CaptureDataChangeConsumer}.
 *
 * @see IgniteCDC
 * @see CaptureDataChangeConsumer
 */
public class WALRecordsConsumer<K, V> {
    /** Ignite logger. */
    private IgniteLogger log;

    /** Data change events consumer. */
    private final CaptureDataChangeConsumer dataConsumer;

    /** Operations types we interested in. */
    private static final EnumSet<GridCacheOperation> OPS_TYPES = EnumSet.of(CREATE, UPDATE, DELETE, TRANSFORM);

    /** Operations filter. */
    private static final IgnitePredicate<? super DataEntry> OPS_FILTER = e -> {
        if (!(e instanceof UnwrappedDataEntry))
            throw new IllegalStateException("Unexpected data entry type[" + e.getClass().getName());

        return OPS_TYPES.contains(e.op());
    };

    /**
     * @param dataConsumer User provided CDC consumer.
     */
    public WALRecordsConsumer(CaptureDataChangeConsumer dataConsumer) {
        this.dataConsumer = dataConsumer;
    }

    /**
     * Handles record from the WAL.
     * If this method return {@code true} then current offset in WAL will be stored and WAL iteration will be
     * started from it on CDC application fail/restart.
     *
     * @param recs WAL records iterator.
     * @param <T> Record type.
     * @return {@code True} if current offset in WAL should be commited.
     */
    public <T extends WALRecord> boolean onRecords(Iterator<T> recs) {
        recs = F.iterator(recs, r -> r, true, r -> {
            //System.out.println(r.type());

            return r.type() == WALRecord.RecordType.DATA_RECORD_V2;
        });

        return dataConsumer.onChange(F.concat(F.iterator(recs, r -> F.iterator(((DataRecord)r).writeEntries(), e -> {
            UnwrappedDataEntry ue = (UnwrappedDataEntry)e;

            ChangeEventType type;

            switch (e.op()) {
                // Combine two types of the events because `CREATE` only generated for first `put`
                // of the key for `TRANSACTIONAL` caches.
                // For `ATOMIC` caches every `put` generate `UPDATE` event.
                case CREATE:
                case UPDATE:
                    type = ChangeEventType.UPDATE;

                    break;

                case DELETE:
                    type = ChangeEventType.DELETE;

                    break;

                default:
                    throw new IllegalStateException("Unexpected operation type[" + e.op());
            }

            GridCacheVersion ver = e.writeVersion();

            ChangeEventOrder ord = new ChangeEventOrder(ver.topologyVersion(), ver.nodeOrderAndDrIdRaw(), ver.order());

            GridCacheVersion replicaVer = ver.conflictVersion();

            if (replicaVer != ver) {
                ord.otherDcOrder(new ChangeEventOrder(
                    replicaVer.topologyVersion(), replicaVer.nodeOrderAndDrIdRaw(), replicaVer.order()));
            }

            return new ChangeEvent(
                ue.unwrappedKey(),
                ue.unwrappedValue(),
                e.primary(),
                e.partitionId(),
                ord,
                type,
                e.cacheId()
            );
        }, true, OPS_FILTER), true)));
    }

    /**
     * Starts the consumer.
     *
     * @param log Logger.
     */
    public void start(IgniteLogger log) {
        this.log = log;

        dataConsumer.start(log);

        if (log.isInfoEnabled())
            log.info("WalRecordsConsumer started[consumer=" + dataConsumer.getClass() + ']');
    }

    /**
     * Stops the consumer.
     * This methods can be invoked only after {@link #start(IgniteLogger)}.
     */
    public void stop() {
        dataConsumer.stop();

        if (log.isInfoEnabled())
            log.info("WalRecordsConsumer stoped[consumer=" + dataConsumer.getClass() + ']');
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WALRecordsConsumer.class, this);
    }
}
