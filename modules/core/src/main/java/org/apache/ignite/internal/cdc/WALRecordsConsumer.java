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
import org.apache.ignite.cdc.CDCConsumer;
import org.apache.ignite.cdc.EntryEvent;
import org.apache.ignite.cdc.EntryEventOrder;
import org.apache.ignite.cdc.EntryEventType;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.UnwrappedDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_RECORD_V2;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.CREATE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;

/**
 * Transform {@link DataEntry} to {@link EntryEvent} and sends it to {@link CDCConsumer}.
 *
 * @see IgniteCDC
 * @see CDCConsumer
 */
public class WALRecordsConsumer<K, V> {
    /** Ignite logger. */
    private IgniteLogger log;

    /** Data change events consumer. */
    private final CDCConsumer<K, V> dataConsumer;

    /** Operations types we interested in. */
    private static final EnumSet<GridCacheOperation> OPS_TYPES = EnumSet.of(CREATE, UPDATE, DELETE, TRANSFORM);

    /** WAL Records filter. */
    private static final IgnitePredicate<WALRecord> DATA_REC_FILTER = r -> r.type() == DATA_RECORD_V2;

    /** Operations filter. */
    private static final IgnitePredicate<? super DataEntry> OPS_FILTER = e -> {
        if (!(e instanceof UnwrappedDataEntry))
            throw new IllegalStateException("Unexpected data entry type[" + e.getClass().getName());

        return OPS_TYPES.contains(e.op());
    };

    /**
     * @param dataConsumer User provided CDC consumer.
     */
    public WALRecordsConsumer(CDCConsumer<K, V> dataConsumer) {
        this.dataConsumer = dataConsumer;
    }

    /**
     * Handles record from the WAL.
     * If this method return {@code true} then current offset in WAL will be stored and WAL iteration will be
     * started from it on CDC application fail/restart.
     *
     * @param records WAL records iterator.
     * @param <T> Record type.
     * @return {@code True} if current offset in WAL should be commited.
     */
    public <T extends WALRecord> boolean onRecords(Iterator<T> recs) {
        return dataConsumer.onChange(F.concat(F.iterator(recs, r -> F.iterator(((DataRecord)r).writeEntries(), e -> {
            UnwrappedDataEntry ue = (UnwrappedDataEntry)e;

            EntryEventType type;

            switch (e.op()) {
                // Combine two types of the events because `CREATE` only generated for first `put`
                // of the key for `TRANSACTIONAL` caches.
                // For `ATOMIC` caches every `put` generate `UPDATE` event.
                case CREATE:
                case UPDATE:
                    type = EntryEventType.UPDATE;

                    break;

                case DELETE:
                    type = EntryEventType.DELETE;

                    break;

                default:
                    throw new IllegalStateException("Unexpected operation type[" + e.op());
            }

            GridCacheVersion ver = e.writeVersion();

            EntryEventOrder ord = new EntryEventOrder(ver.topologyVersion(), ver.nodeOrderAndDrIdRaw(), ver.order());

            GridCacheVersion replicaVer = ver.conflictVersion();

            if (replicaVer != ver) {
                ord.otherDcOrder(new EntryEventOrder(
                    replicaVer.topologyVersion(), replicaVer.nodeOrderAndDrIdRaw(), replicaVer.order()));
            }

            return new EntryEvent<>(
                (K)ue.unwrappedKey(),
                (V)ue.unwrappedValue(),
                e.primary(),
                e.partitionId(),
                ord,
                type,
                e.cacheId(),
                e.expireTime()
            );
        }, true, OPS_FILTER), true, DATA_REC_FILTER)));
    }

    /**
     * @return Consumer ID.
     */
    public String id() {
        return dataConsumer.id();
    }

    /**
     * Starts the consumer.
     *
     * @param configuration Ignite configuration.
     */
    public void start(IgniteConfiguration configuration, IgniteLogger log) {
        this.log = log;

        dataConsumer.start(configuration, log);

        log.info("DataChangeConsumer started[id=" + dataConsumer.id() + ']');
    }

    /**
     * @return {@code True} if entry key and value should be keeped in binary format.
     */
    public boolean keepBinary() {
        return dataConsumer.keepBinary();
    }

    /**
     * Stops the consumer.
     * This methods can be invoked only after {@link #start(IgniteConfiguration, IgniteLogger)}.
     */
    public void stop() {
        dataConsumer.stop();

        log.info("DataChangeConsumer stoped[id=" + dataConsumer.id() + ']');
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getSimpleName() + "[id=" + id() + ']';
    }
}
