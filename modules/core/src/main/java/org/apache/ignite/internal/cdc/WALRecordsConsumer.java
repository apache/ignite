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
import org.apache.ignite.cdc.ChangeDataCaptureConsumer;
import org.apache.ignite.cdc.ChangeDataCaptureEvent;
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
 * Transform {@link DataEntry} to {@link ChangeDataCaptureEvent} and sends it to {@link ChangeDataCaptureConsumer}.
 *
 * @see IgniteCDC
 * @see ChangeDataCaptureConsumer
 */
public class WALRecordsConsumer<K, V> {
    /** Ignite logger. */
    private IgniteLogger log;

    /** Data change events consumer. */
    private final ChangeDataCaptureConsumer dataConsumer;

    /** Operations types we interested in. */
    private static final EnumSet<GridCacheOperation> OPS_TYPES = EnumSet.of(CREATE, UPDATE, DELETE, TRANSFORM);

    /** Operations filter. */
    private static final IgnitePredicate<? super DataEntry> OPS_FILTER = e -> {
        if (!(e instanceof UnwrappedDataEntry))
            throw new IllegalStateException("Unexpected data entry type[" + e.getClass().getName());

        if ((e.flags() & DataEntry.PRELOAD_FLAG) != 0 ||
            (e.flags() & DataEntry.FROM_STORE_FLAG) != 0)
            return false;

        return OPS_TYPES.contains(e.op());
    };

    /**
     * @param dataConsumer User provided CDC consumer.
     */
    public WALRecordsConsumer(ChangeDataCaptureConsumer dataConsumer) {
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
        recs = F.iterator(recs, r -> r, true, r -> r.type() == WALRecord.RecordType.DATA_RECORD_V2);

        return dataConsumer.onChange(F.concat(F.iterator(recs, r -> F.iterator(((DataRecord)r).writeEntries(), e -> {
            UnwrappedDataEntry ue = (UnwrappedDataEntry)e;

            GridCacheVersion ver = e.writeVersion();

            ChangeEventOrderImpl ord =
                new ChangeEventOrderImpl(ver.topologyVersion(), ver.nodeOrderAndDrIdRaw(), ver.order());

            GridCacheVersion replicaVer = ver.conflictVersion();

            if (replicaVer != ver) {
                ord.otherDcOrder(new ChangeEventOrderImpl(
                    replicaVer.topologyVersion(), replicaVer.nodeOrderAndDrIdRaw(), replicaVer.order()));
            }

            return new ChangeDataCaptureEventImpl(
                ue.unwrappedKey(),
                ue.unwrappedValue(),
                (e.flags() & DataEntry.PRIMARY_FLAG) != 0,
                e.partitionId(),
                ord,
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
