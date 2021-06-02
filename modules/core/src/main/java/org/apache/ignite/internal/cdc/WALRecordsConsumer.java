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
import java.util.NoSuchElementException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cdc.ChangeDataCaptureConsumer;
import org.apache.ignite.cdc.ChangeDataCaptureEvent;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.UnwrappedDataEntry;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
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
 * @see ChangeDataCapture
 * @see ChangeDataCaptureConsumer
 */
public class WALRecordsConsumer<K, V> {
    /** Ignite logger. */
    private final IgniteLogger log;

    /** Data change events consumer. */
    private final ChangeDataCaptureConsumer consumer;

    /** Operations types we interested in. */
    private static final EnumSet<GridCacheOperation> OPERATIONS_TYPES = EnumSet.of(CREATE, UPDATE, DELETE, TRANSFORM);

    /** Operations filter. */
    private static final IgnitePredicate<? super DataEntry> OPERATIONS_FILTER = e -> {
        if (!(e instanceof UnwrappedDataEntry))
            throw new IllegalStateException("Unexpected data entry [type=" + e.getClass().getName() + ']');

        if ((e.flags() & DataEntry.PRELOAD_FLAG) != 0 ||
            (e.flags() & DataEntry.FROM_STORE_FLAG) != 0)
            return false;

        return OPERATIONS_TYPES.contains(e.op());
    };

    /**
     * @param consumer User provided CDC consumer.
     * @param log Logger.
     */
    public WALRecordsConsumer(ChangeDataCaptureConsumer consumer, IgniteLogger log) {
        this.consumer = consumer;
        this.log = log;
    }

    /**
     * Handles record from the WAL.
     * If this method return {@code true} then current offset in WAL will be stored and WAL iteration will be
     * started from it on CDC application fail/restart.
     *
     * @param recs WAL records iterator.
     * @return {@code True} if current offset in WAL should be commited.
     */
    public boolean onRecords(Iterator<DataRecord> recs) {
        Iterator<ChangeDataCaptureEvent> evts = new Iterator<ChangeDataCaptureEvent>() {
            /** */
            private Iterator<ChangeDataCaptureEvent> entries;

            @Override public boolean hasNext() {
                advance();

                return hasCurrent();
            }

            @Override public ChangeDataCaptureEvent next() {
                advance();

                if (!hasCurrent())
                    throw new NoSuchElementException();

                return entries.next();
            }

            private void advance() {
                if (hasCurrent())
                    return;

                while (recs.hasNext()) {
                    entries =
                        F.iterator(recs.next().writeEntries().iterator(), this::transform, true, OPERATIONS_FILTER);

                    if (entries.hasNext())
                        break;

                    entries = null;
                }
            }

            private boolean hasCurrent() {
                return entries != null && entries.hasNext();
            }

            /** */
            private ChangeDataCaptureEvent transform(DataEntry e) {
                UnwrappedDataEntry ue = (UnwrappedDataEntry)e;

                return new ChangeDataCaptureEventImpl(
                    ue.unwrappedKey(),
                    ue.unwrappedValue(),
                    (e.flags() & DataEntry.PRIMARY_FLAG) != 0,
                    e.partitionId(),
                    e.writeVersion(),
                    e.cacheId()
                );
            }
        };

        return consumer.onEvents(evts);
    }

    /**
     * Starts the consumer.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void start() throws IgniteCheckedException {
        consumer.start();

        if (log.isDebugEnabled())
            log.debug("WalRecordsConsumer started [consumer=" + consumer.getClass() + ']');
    }

    /**
     * Stops the consumer.
     * This methods can be invoked only after {@link #start()}.
     */
    public void stop() {
        consumer.stop();

        if (log.isInfoEnabled())
            log.info("WalRecordsConsumer stoped[consumer=" + consumer.getClass() + ']');
    }

    /** @return Change Data Capture Consumer. */
    public ChangeDataCaptureConsumer consumer() {
        return consumer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WALRecordsConsumer.class, this);
    }
}
