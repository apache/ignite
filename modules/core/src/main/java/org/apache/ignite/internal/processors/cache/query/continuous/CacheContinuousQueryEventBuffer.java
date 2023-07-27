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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.LongUnaryOperator;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.SystemProperty;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CacheContinuousQueryEventBuffer {
    /** @see #IGNITE_CONTINUOUS_QUERY_PENDING_BUFF_SIZE */
    public static final int DFLT_CONTINUOUS_QUERY_PENDING_BUFF_SIZE = 10_000;

    /** @see #IGNITE_CONTINUOUS_QUERY_SERVER_BUFFER_SIZE */
    public static final int DFLT_CONTINUOUS_QUERY_SERVER_BUFFER_SIZE = 1000;

    /** */
    @SystemProperty(value = "The max size of the buffer with pending continuous queries events",
        type = Long.class, defaults = "" + DFLT_CONTINUOUS_QUERY_PENDING_BUFF_SIZE)
    public static final String IGNITE_CONTINUOUS_QUERY_PENDING_BUFF_SIZE = "IGNITE_CONTINUOUS_QUERY_PENDING_BUFF_SIZE";

    /** */
    @SystemProperty(value = "Continuous queries batch buffer size", type = Long.class,
        defaults = "" + DFLT_CONTINUOUS_QUERY_SERVER_BUFFER_SIZE)
    public static final String IGNITE_CONTINUOUS_QUERY_SERVER_BUFFER_SIZE = "IGNITE_CONTINUOUS_QUERY_SERVER_BUFFER_SIZE";

    /** Maximum size of buffer for pending events. Default value is {@code 10_000}. */
    public static final int MAX_PENDING_BUFF_SIZE =
        IgniteSystemProperties.getInteger(IGNITE_CONTINUOUS_QUERY_PENDING_BUFF_SIZE, DFLT_CONTINUOUS_QUERY_PENDING_BUFF_SIZE);

    /** Batch buffer size. */
    private static final int BUF_SIZE =
        IgniteSystemProperties.getInteger(IGNITE_CONTINUOUS_QUERY_SERVER_BUFFER_SIZE, DFLT_CONTINUOUS_QUERY_SERVER_BUFFER_SIZE);

    /** */
    private static final Object RETRY = new Object();

    /** Continuous query category logger. */
    private final IgniteLogger log;

    /** Function returns current partition counter related to this buffer. */
    private final LongUnaryOperator currPartCntr;

    /** Batch of entries currently being collected to send to the remote. */
    private final AtomicReference<Batch> curBatch = new AtomicReference<>();

    /** Queue for keeping backup entries which partition counter less the counter processing by current batch. */
    private final Deque<CacheContinuousQueryEntry> backupQ = new ConcurrentLinkedDeque<>();

    /** Entries which are waiting for being processed. */
    private final ConcurrentSkipListMap<Long, CacheContinuousQueryEntry> pending = new ConcurrentSkipListMap<>();

    /**
     * The size method of the pending ConcurrentSkipListMap is not a constant-time operation. Since each
     * entry processed under the GridCacheMapEntry lock it's necessary to maintain the size of map explicitly.
     */
    private final AtomicInteger pendingCurrSize = new AtomicInteger();

    /** Last seen ack partition counter tracked by the CQ handler partition recovery queue. */
    private final GridAtomicLong ackedUpdCntr = new GridAtomicLong(0);

    /**
     * @param currPartCntr Current partition counter.
     * @param log Continuous query category logger.
     */
    CacheContinuousQueryEventBuffer(LongUnaryOperator currPartCntr, IgniteLogger log) {
        this.currPartCntr = currPartCntr;
        this.log = log;
    }

    /**
     * @param log Continuous query category logger.
     */
    CacheContinuousQueryEventBuffer(IgniteLogger log) {
        this((backup) -> 0, log);
    }

    /**
     * @param updateCntr Acknowledged counter.
     */
    void cleanupOnAck(long updateCntr) {
        backupQ.removeIf(backupEntry -> backupEntry.updateCounter() <= updateCntr);

        ackedUpdCntr.setIfGreater(updateCntr);
    }

    /**
     * @param filteredFactory Factory which will produce entries.
     * @return Collection of backup entries.
     */
    @Nullable Collection<CacheContinuousQueryEntry> flushOnExchange(
        BiFunction<Long, Long, CacheContinuousQueryEntry> filteredFactory
    ) {
        Map<Long, CacheContinuousQueryEntry> ret = new TreeMap<>();

        int size = backupQ.size();

        for (int i = 0; i < size; i++) {
            CacheContinuousQueryEntry e = backupQ.pollFirst();

            if (e == null)
                break;

            ret.put(e.updateCounter(), e);
        }

        Batch batch = curBatch.get();

        if (batch != null)
            ret.putAll(batch.flushCurrentEntries(filteredFactory));

        for (CacheContinuousQueryEntry e : pending.values())
            ret.put(e.updateCounter(), e);

        return ret.isEmpty() ? null : ret.values();
    }

    /**
     * For test purpose only.
     *
     * @return Current number of filtered events.
     */
    long currentFiltered() {
        Batch batch = curBatch.get();

        return batch != null ? batch.filtered : 0;
    }

    /**
     * @param e Entry to process.
     * @param backup Backup entry flag.
     * @return Collected entries to pass to listener (single entry or entries list).
     */
    @Nullable Object processEntry(CacheContinuousQueryEntry e, boolean backup) {
        return process0(e.updateCounter(), e, backup);
    }

    /**
     * @param backup Backup entry flag.
     * @param cntr Entry counter.
     * @param entry Entry.
     * @return Collected entries.
     */
    private Object process0(long cntr, CacheContinuousQueryEntry entry, boolean backup) {
        assert cntr >= 0 : cntr;

        Batch batch;
        Object res = null;

        for (;;) {
            // Set batch only if batch is null (first attempt).
            batch = initBatch(backup);

            if (batch == null || cntr < batch.startCntr) {
                if (backup && cntr > ackedUpdCntr.get())
                    backupQ.add(entry);

                return backup ? null : entry;
            }

            if (cntr <= batch.endCntr) {
                res = batch.processEntry0(null, cntr, entry, backup);

                if (res == RETRY)
                    continue;
            }
            else {
                if (batch.endCntr < ackedUpdCntr.get() && batch.tryRollOver() == RETRY)
                    continue;

                pendingCurrSize.incrementAndGet();
                pending.put(cntr, entry);

                if (pendingCurrSize.get() > MAX_PENDING_BUFF_SIZE) {
                    synchronized (pending) {
                        if (pendingCurrSize.get() <= MAX_PENDING_BUFF_SIZE)
                            break;

                        LT.warn(log, "Buffer for pending events reached max of its size " +
                            "[cacheId=" + entry.cacheId() + ", maxSize=" + MAX_PENDING_BUFF_SIZE +
                            ", partId=" + entry.partition() + ']');

                        // Remove first BUFF_SIZE keys.
                        int keysToRemove = BUF_SIZE;

                        Iterator<Map.Entry<Long, CacheContinuousQueryEntry>> iter = pending.entrySet().iterator();

                        while (iter.hasNext() && keysToRemove > 0) {
                            CacheContinuousQueryEntry entry0 = iter.next().getValue();

                            // Discard messages on backup and send to client if primary.
                            if (!backup)
                                res = addResult(res, entry0.copyWithDataReset(), backup);

                            iter.remove();
                            pendingCurrSize.decrementAndGet();
                            keysToRemove--;
                        }
                    }
                }
            }

            break;
        }

        Batch batch0 = curBatch.get();

        // Batch has been changed on entry processing to the new one.
        while (batch != batch0) {
            batch = batch0;

            res = processPending(res, batch, backup);

            batch0 = curBatch.get();
        }

        return res;
    }

    /**
     * @param backup {@code True} if backup entry.
     * @return Current batch.
     */
    private Batch initBatch(boolean backup) {
        while (curBatch.get() == null) {
            long curCntr = currPartCntr.applyAsLong(backup ? 1 : 0);

            if (curCntr == -1)
                return null;

            curBatch.compareAndSet(null, new Batch(curCntr + 1, 0L, new CacheContinuousQueryEntry[BUF_SIZE]));
        }

        return curBatch.get();
    }

    /**
     * @param res Current result.
     * @param batch Current batch.
     * @param backup Backup entry flag.
     * @return New result.
     */
    @Nullable private Object processPending(@Nullable Object res, Batch batch, boolean backup) {
        if (pending.floorKey(batch.endCntr) == null)
            return res;

        synchronized (pending) {
            for (Map.Entry<Long, CacheContinuousQueryEntry> p : pending.headMap(batch.endCntr, true).entrySet()) {
                long cntr = p.getKey();

                assert cntr <= batch.endCntr;

                if (pending.remove(cntr) == null)
                    continue;

                if (cntr < batch.startCntr)
                    res = addResult(res, p.getValue(), backup);
                else
                    res = batch.processEntry0(res, p.getKey(), p.getValue(), backup);

                pendingCurrSize.decrementAndGet();
            }

            return res;
        }
    }

    /**
     * @param res Current result.
     * @param entry Entry to add.
     * @param backup Backup entry flag.
     * @return Updated result.
     */
    @Nullable private Object addResult(@Nullable Object res, CacheContinuousQueryEntry entry, boolean backup) {
        if (res == null) {
            if (backup)
                backupQ.add(entry);
            else
                res = entry;
        }
        else {
            assert !backup;

            List<CacheContinuousQueryEntry> resList;

            if (res instanceof CacheContinuousQueryEntry) {
                resList = new ArrayList<>();

                resList.add((CacheContinuousQueryEntry)res);
            }
            else {
                assert res instanceof List : res;

                resList = (List<CacheContinuousQueryEntry>)res;
            }

            resList.add(entry);

            res = resList;
        }

        return res;
    }

    /** */
    int backupQueueSize() {
        return backupQ.size();
    }

    /**
     *
     */
    private class Batch {
        /** */
        private long filtered;

        /** */
        private final long startCntr;

        /** */
        private final long endCntr;

        /** */
        private int lastProc = -1;

        /** */
        private CacheContinuousQueryEntry[] entries;

        /**
         * @param filtered Number of filtered events before this batch.
         * @param entries Entries array.
         * @param startCntr Start counter.
         */
        Batch(long startCntr, long filtered, CacheContinuousQueryEntry[] entries) {
            assert startCntr >= 0;
            assert filtered >= 0;

            this.startCntr = startCntr;
            this.filtered = filtered;
            this.entries = entries;

            endCntr = startCntr + BUF_SIZE - 1;
        }

        /**
         * @param filteredFactory Factory which produces filtered entries.
         * @return Map of collected entries.
         */
        synchronized Map<Long, CacheContinuousQueryEntry> flushCurrentEntries(
            BiFunction<Long, Long, CacheContinuousQueryEntry> filteredFactory
        ) {
            if (entries == null || filteredFactory == null)
                return Collections.emptyMap();

            Map<Long, CacheContinuousQueryEntry> res = new HashMap<>();

            long filtered = this.filtered;
            long cntr = startCntr;

            for (int i = 0; i < entries.length; i++) {
                CacheContinuousQueryEntry e = entries[i];

                CacheContinuousQueryEntry flushEntry = null;

                if (e == null) {
                    if (filtered != 0) {
                        flushEntry = filteredFactory.apply(cntr - 1, filtered - 1);

                        filtered = 0;
                    }
                }
                else {
                    if (e.isFiltered())
                        filtered++;
                    else {
                        flushEntry = new CacheContinuousQueryEntry(e.cacheId(),
                            e.eventType(),
                            e.key(),
                            e.value(),
                            e.oldValue(),
                            e.isKeepBinary(),
                            e.partition(),
                            e.updateCounter(),
                            e.topologyVersion(),
                            e.flags());

                        flushEntry.filteredCount(filtered);

                        filtered = 0;
                    }
                }

                if (flushEntry != null)
                    res.put(flushEntry.updateCounter(), flushEntry);

                cntr++;
            }

            if (filtered != 0L) {
                CacheContinuousQueryEntry flushEntry = filteredFactory.apply(cntr - 1, filtered - 1);

                res.put(flushEntry.updateCounter(), flushEntry);
            }

            return res;
        }

        /**
         * @param res Current result.
         * @param cntr Entry counter.
         * @param entry Entry.
         * @param backup Backup entry flag.
         * @return New result.
         */
        @Nullable private Object processEntry0(
            @Nullable Object res,
            long cntr,
            CacheContinuousQueryEntry entry,
            boolean backup) {
            int pos = (int)(cntr - startCntr);

            synchronized (this) {
                if (entries == null)
                    return RETRY;

                entry = entry.copyWithDataReset();

                entries[pos] = entry;

                int next = lastProc + 1;
                long ackedUpdCntr0 = ackedUpdCntr.get();

                if (next == pos) {
                    for (int i = next; i < entries.length; i++) {
                        CacheContinuousQueryEntry entry0 = entries[i];

                        if (entry0 != null) {
                            if (!entry0.isFiltered()) {
                                entry0.filteredCount(filtered);

                                filtered = 0;

                                res = addResult(res, entry0, backup);
                            }
                            else
                                filtered++;

                            pos = i;
                        }
                        else
                            break;
                    }

                    lastProc = pos;

                    if (pos == entries.length - 1)
                        rollOver(startCntr + BUF_SIZE, filtered);
                }
                else if (endCntr < ackedUpdCntr0)
                    rollOver(ackedUpdCntr0 + 1, 0);

                return res;
            }
        }

        /** Try to change batch to the new one. */
        private synchronized Object tryRollOver() {
            if (entries == null)
                return RETRY;

            long ackedUpdCntr0 = ackedUpdCntr.get();

            if (endCntr < ackedUpdCntr0) {
                rollOver(ackedUpdCntr0 + 1, 0);

                return RETRY;
            }

            return null;
        }

        /**
         * @param startCntr Start batch position.
         * @param filtered Number of filtered entries prior start position.
         */
        private void rollOver(long startCntr, long filtered) {
            Arrays.fill(entries, null);

            Batch nextBatch = new Batch(startCntr,
                filtered,
                entries);

            entries = null;

            boolean changed = curBatch.compareAndSet(this, nextBatch);

            assert changed;
        }
    }
}
