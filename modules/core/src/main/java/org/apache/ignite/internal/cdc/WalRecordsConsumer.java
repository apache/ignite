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
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cdc.CdcCacheEvent;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.UnwrappedDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.CREATE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;

/**
 * Transform {@link DataEntry} to {@link CdcEvent} and sends it to {@link CdcConsumer}.
 *
 * @see CdcMain
 * @see CdcConsumer
 */
public class WalRecordsConsumer<K, V> {
    /** Events count metric name. */
    public static final String EVTS_CNT = "EventsCount";

    /** Last event time metric name. */
    public static final String LAST_EVT_TIME = "LastEventTime";

    /** Ignite logger. */
    private final IgniteLogger log;

    /** Data change events consumer. */
    private final CdcConsumer consumer;

    /** Event count metric */
    private AtomicLongMetric evtsCnt;

    /** Timestamp of last event process. */
    private AtomicLongMetric lastEvtTs;

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

    /** Event transformer. */
    private static final IgniteClosure<DataEntry, CdcEvent> CDC_EVENT_TRANSFORMER = e -> {
        UnwrappedDataEntry ue = (UnwrappedDataEntry)e;

        return new CdcEventImpl(
            ue.unwrappedKey(),
            ue.unwrappedValue(),
            (e.flags() & DataEntry.PRIMARY_FLAG) != 0,
            e.partitionId(),
            e.writeVersion(),
            e.cacheId()
        );
    };

    /**
     * @param consumer User provided CDC consumer.
     * @param log Logger.
     */
    public WalRecordsConsumer(CdcConsumer consumer, IgniteLogger log) {
        this.consumer = consumer;
        this.log = log;
    }

    /**
     * Handles data entries.
     * If this method return {@code true} then current offset in WAL and {@link DataEntry} index inside
     * {@link DataRecord} will be stored and WAL iteration will be started from it on CDC application fail/restart.
     *
     * @param entries Data entries iterator.
     * @return {@code True} if current offset in WAL should be commited.
     */
    public boolean onRecords(Iterator<DataEntry> entries) {
        Iterator<CdcEvent> evts = F.iterator(new Iterator<DataEntry>() {
            @Override public boolean hasNext() {
                return entries.hasNext();
            }

            @Override public DataEntry next() {
                DataEntry next = entries.next();

                evtsCnt.increment();

                lastEvtTs.value(System.currentTimeMillis());

                return next;
            }
        }, CDC_EVENT_TRANSFORMER, true, OPERATIONS_FILTER);

        return consumer.onEvents(evts);
    }

    /**
     * Handles new binary types.
     * @param types Binary types iterator.
     */
    public void onTypes(Iterator<BinaryType> types) {
        consumer.onTypes(types);
    }

    /**
     * Handles new mappings.
     * @param mappings Mappings iterator.
     */
    public void onMappings(Iterator<TypeMapping> mappings) {
        consumer.onMappings(mappings);
    }

    /**
     * Handles new cache events.
     *
     * @param cacheEvts Cache events iterator.
     */
    public void onCacheEvents(Iterator<CdcCacheEvent> cacheEvts) {
        consumer.onCacheChange(cacheEvts);
    }

    /**
     * Handles destroy cache events.
     *
     * @param caches Destroyed cache iterator.
     */
    public void onCacheDestroyEvents(Iterator<Integer> caches) {
        consumer.onCacheDestroy(caches);
    }

    /**
     * Starts the consumer.
     *
     * @param cdcReg CDC metric registry.
     * @param cdcConsumerReg CDC consumer metric registry.
     * @throws IgniteCheckedException If failed.
     */
    public void start(MetricRegistry cdcReg, MetricRegistry cdcConsumerReg) throws IgniteCheckedException {
        consumer.start(cdcConsumerReg);

        evtsCnt = cdcReg.longMetric(EVTS_CNT, "Count of events processed by the consumer");
        lastEvtTs = cdcReg.longMetric(LAST_EVT_TIME, "Time of the last event process");

        if (log.isDebugEnabled())
            log.debug("WalRecordsConsumer started [consumer=" + consumer.getClass() + ']');
    }

    /**
     * Stops the consumer.
     * This methods can be invoked only after {@link #start(MetricRegistry, MetricRegistry)}.
     */
    public void stop() {
        consumer.stop();

        if (log.isInfoEnabled())
            log.info("WalRecordsConsumer stopped [consumer=" + consumer.getClass() + ']');
    }

    /**
     * Checks that consumer still alive.
     * This method helps to determine {@link CdcConsumer} errors in case {@link CdcEvent} is rare or source cluster is down.
     *
     * @return {@code True} in case consumer alive, {@code false} otherwise.
     */
    public boolean alive() {
        return consumer.alive();
    }

    /** @return Change Data Capture Consumer. */
    public CdcConsumer consumer() {
        return consumer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WalRecordsConsumer.class, this);
    }

    /** Iterator over {@link DataEntry}. */
    public static class DataEntryIterator implements Iterator<DataEntry>, AutoCloseable {
        /** WAL iterator. */
        private final WALIterator walIter;

        /** Current preloaded WAL record. */
        private IgniteBiTuple<WALPointer, WALRecord> curRec;

        /** */
        private DataEntry next;

        /** Index of {@link #next} inside WAL record. */
        private int entryIdx;

        /** @param walIter WAL iterator. */
        DataEntryIterator(WALIterator walIter) {
            this.walIter = walIter;

            advance();
        }

        /** @return Current state. */
        T2<WALPointer, Integer> state() {
            return hasNext() ?
                new T2<>(curRec.get1(), entryIdx) :
                curRec != null
                    ? new T2<>(curRec.get1().next(), 0)
                    : walIter.lastRead().map(ptr -> new T2<>(ptr.next(), 0)).orElse(null);
        }

        /** Initialize state. */
        void init(int idx) {
            for (int i = 0; i < idx; i++) {
                if (!hasNext())
                    throw new IgniteException("Failed to restore entry index [idx=" + idx + ", rec=" + curRec + ']');

                next();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return next != null;
        }

        /** {@inheritDoc} */
        @Override public DataEntry next() {
            if (!hasNext())
                throw new NoSuchElementException();

            DataEntry e = next;

            next = null;

            advance();

            return e;
        }

        /** */
        private void advance() {
            if (curRec != null) {
                entryIdx++;

                DataRecord rec = (DataRecord)curRec.get2();

                if (entryIdx < rec.entryCount()) {
                    next = rec.get(entryIdx);

                    return;
                }

                entryIdx = 0;
            }

            if (!walIter.hasNext())
                return;

            curRec = walIter.next();

            next = ((DataRecord)curRec.get2()).get(entryIdx);
        }

        /** {@inheritDoc} */
        @Override public void close() throws IgniteCheckedException {
            walIter.close();
        }
    }
}
