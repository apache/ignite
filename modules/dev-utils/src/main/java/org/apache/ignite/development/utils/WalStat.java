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
package org.apache.ignite.development.utils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.transactions.TransactionState;
import sun.nio.ch.DirectBuffer;

/**
 * Statistic for overall WAL file
 */
public class WalStat {
    /** Display max: top lines before merging other values */
    public static final int DISPLAY_MAX = 80;

    /** Tx stat. */
    private TxWalStat txStat = new TxWalStat();

    /** Segments source */
    private final Map<String, RecordSizeCountStat> segmentsSrcProcessed = new TreeMap<>();

    /** Segments processed */
    private final Map<String, RecordSizeCountStat> segmentsProcessed = new TreeMap<>();

    /** Rec type sizes. */
    private final Map<String, RecordSizeCountStat> recTypeSizes = new TreeMap<>();

    /** Data Record: entries count */
    private final Map<Integer, RecordSizeCountStat> dataRecordEntriesCnt = new TreeMap<>();

    /** Data Record: is under TX */
    private final Map<Boolean, RecordSizeCountStat> dataRecordUnderTx = new TreeMap<>();

    /** Data Entry: operation performed */
    private final Map<String, RecordSizeCountStat> dataEntryOperation = new TreeMap<>();

    /** Data Entry: cache groups. */
    private final Map<Integer, RecordSizeCountStat> dataEntryCacheId = new TreeMap<>();

    /** Tx Record: action */
    private final Map<String, RecordSizeCountStat> txRecordAct = new TreeMap<>();

    /** Tx Record: participating primary nodes */
    private final Map<Integer, RecordSizeCountStat> txRecordPrimNodesCnt = new TreeMap<>();

    /** Tx Record: participating primary nodes */
    private final Map<Integer, RecordSizeCountStat> txRecordNodesCnt = new TreeMap<>();

    /** Page snapshot types. */
    private final Map<String, RecordSizeCountStat> pageSnapshotTypes = new TreeMap<>();

    /** Page snapshot indexes. */
    private final Map<String, RecordSizeCountStat> pageSnapshotIndexes = new TreeMap<>();

    /** Page snapshot cache groups. */
    private final Map<Integer, RecordSizeCountStat> pageSnapshotCacheGrp = new TreeMap<>();

    /** Page snapshot: partition ID. */
    private final Map<Integer, RecordSizeCountStat> pageSnapshotPartId = new TreeMap<>();

    /**
     * @param key
     * @param record
     * @param map
     * @param <K>
     */
    private <K> void computeStatIfAbsent(K key, WALRecord record, Map<K, RecordSizeCountStat> map) {
        final int size = record.size();
        computeStatIfAbsent(key, map, size);
    }

    /**
     * @param key
     * @param map
     * @param size Negative value of size means size is unknown for current row.
     * @param <K>
     */
    private <K> void computeStatIfAbsent(K key, Map<K, RecordSizeCountStat> map, int size) {
        final RecordSizeCountStat val = map.get(key);
        final RecordSizeCountStat recordStat = val == null ? new RecordSizeCountStat() : val;
        recordStat.occurrence(size);
        map.put(key, recordStat);
    }

    /**
     * @param type
     * @param record
     * @param walPointer
     * @param workDir
     */
    void registerRecord(WALRecord.RecordType type, WALRecord record,
        WALPointer walPointer, boolean workDir) {
        if (type == WALRecord.RecordType.PAGE_RECORD)
            registerPageSnapshot((PageSnapshot)record);
        else if (type == WALRecord.RecordType.DATA_RECORD)
            registerDataRecord((DataRecord)record);
        else if (type == WALRecord.RecordType.TX_RECORD)
            registerTxRecord((TxRecord)record);

        computeStatIfAbsent(type.toString(), record, recTypeSizes);

        if (walPointer instanceof FileWALPointer) {
            final FileWALPointer fPtr = (FileWALPointer)walPointer;

            final long segment = fPtr.index();

            computeStatIfAbsent(Long.toString(segment), record, segmentsProcessed);
            computeStatIfAbsent(workDir ? "work" : "archive", record, segmentsSrcProcessed);
        }
    }

    /**
     * @param txRecord
     */
    private void registerTxRecord(TxRecord txRecord) {
        final TransactionState state = txRecord.state();

        computeStatIfAbsent(state.toString(), txRecord, txRecordAct);

        int totalNodes = 0;
        final Map<Short, Collection<Short>> map = txRecord.participatingNodes();
        if (map != null) {
            computeStatIfAbsent(map.size(), txRecord, txRecordPrimNodesCnt);

            final HashSet<Object> set = new HashSet<>(150);

            for (Map.Entry<Short, Collection<Short>> next : map.entrySet()) {
                set.add(next.getKey());
                set.addAll(next.getValue());
            }

            totalNodes = set.size();

            computeStatIfAbsent(totalNodes, txRecord, txRecordNodesCnt);
        }

        final GridCacheVersion ver = txRecord.nearXidVersion();
        if (ver != null) {
            switch (state) {
                case PREPARING:
                case PREPARED:
                    txStat.start(ver, map != null ? map.size() : 0, totalNodes);
                    break;
                case COMMITTED:
                    txStat.close(ver, true);
                    break;
                default:
                    txStat.close(ver, false);
            }
        }
    }

    /**
     * @param record
     */
    private void registerPageSnapshot(PageSnapshot record) {
        FullPageId fullPageId = record.fullPageId();
        long pageId = fullPageId.pageId();
        final int idx = PageIdUtils.pageIndex(pageId);

        final String type = getType(record);
        computeStatIfAbsent(type, record, pageSnapshotTypes);

        final String idxAsStr = idx <= 100 ? Integer.toString(idx) : ">100";
        computeStatIfAbsent(idxAsStr, record, pageSnapshotIndexes);
        computeStatIfAbsent(fullPageId.groupId(), record, pageSnapshotCacheGrp);

        final int partId = PageIdUtils.partId(pageId);
        computeStatIfAbsent(partId, record, pageSnapshotPartId);
    }

    /**
     * @param record
     */
    private void registerDataRecord(DataRecord record) {
        final List<DataEntry> dataEntries = record.writeEntries();
        if (!dataEntries.isEmpty()) {
            boolean underTx = false;
            for (DataEntry next : dataEntries) {
                final int size = dataEntries.size() > 1 ? -1 : record.size();
                computeStatIfAbsent(next.op().toString(), dataEntryOperation, size);

                computeStatIfAbsent(next.cacheId(), dataEntryCacheId, size);

                txStat.entry(next);

                underTx |= next.nearXidVersion() != null;
            }

            computeStatIfAbsent(underTx, record, dataRecordUnderTx);
        }

        computeStatIfAbsent(dataEntries.size(), record, dataRecordEntriesCnt);
    }

    /**
     * @param record
     * @return
     */
    private String getType(PageSnapshot record) {
        final byte[] pageData = record.pageData();
        ByteBuffer buf = ByteBuffer.allocateDirect(pageData.length);

        try {
            buf.order(ByteOrder.nativeOrder());
            buf.put(pageData);

            long addr = GridUnsafe.bufferAddress(buf);

            int type = PageIO.getType(addr);
            int ver = PageIO.getVersion(addr);

            return PageIO.getPageIO(type, ver).getClass().getSimpleName();
        }
        catch (IgniteCheckedException ignored) {
        }
        finally {
            ((DirectBuffer)buf).cleaner().clean();
        }
        return "";
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        final StringBuilder sb = new StringBuilder();

        printSizeCountMap(sb, "WAL Segments: sources", segmentsSrcProcessed);
        printSizeCountMap(sb, "WAL Segments: processed", segmentsProcessed);

        printSizeCountMap(sb, "Record type", recTypeSizes);

        printSizeCountMap(sb, "Tx Record: Action", txRecordAct);
        printSizeCountMap(sb, "Tx Record: Primary nodes count", txRecordPrimNodesCnt);
        printSizeCountMap(sb, "Tx Record: Nodes count", txRecordNodesCnt);

        printSizeCountMap(sb, "Data Record: Entries count", dataRecordEntriesCnt);
        printSizeCountMap(sb, "Data Record: Under TX", dataRecordUnderTx);

        printSizeCountMap(sb, "Data Entry: Operations", dataEntryOperation);
        printSizeCountMap(sb, "Data Entry: Cache ID", dataEntryCacheId);

        printSizeCountMap(sb, "Page Snapshot: Page Types", pageSnapshotTypes);
        printSizeCountMap(sb, "Page Snapshot: Indexes", pageSnapshotIndexes);
        printSizeCountMap(sb, "Page Snapshot: Cache Groups", pageSnapshotCacheGrp);
        printSizeCountMap(sb, "Page Snapshot: Partition ID", pageSnapshotPartId);

        sb.append(txStat.toString());

        return sb.toString();
    }

    /**
     * @param sb
     * @param mapName
     * @param map
     */
    private void printSizeCountMap(StringBuilder sb, String mapName, Map<?, RecordSizeCountStat> map) {
        sb.append(mapName).append(": \n");
        sb.append("key\tsize\tcount\tavg.size");
        sb.append("\n");

        final ArrayList<? extends Map.Entry<?, RecordSizeCountStat>> entries = new ArrayList<>(map.entrySet());
        Collections.sort(entries, new Comparator<Map.Entry<?, RecordSizeCountStat>>() {
            @Override public int compare(Map.Entry<?, RecordSizeCountStat> o1, Map.Entry<?, RecordSizeCountStat> o2) {
                return -Integer.compare(o1.getValue().getCount(), o2.getValue().getCount());
            }
        });

        RecordSizeCountStat others = new RecordSizeCountStat();
        int otherRecords = 0;
        int cnt = 0;
        for (Map.Entry<?, RecordSizeCountStat> next : entries) {
            if (cnt < DISPLAY_MAX) {
                sb.append(next.getKey()).append("\t").append(next.getValue()).append("\t");
                sb.append("\n");
            }
            else {
                otherRecords++;
                others.add(next.getValue());
            }
            cnt++;
        }
        if (otherRecords > 0) {
            sb.append("... other ").append(otherRecords).append(" values").append("\t").append(others).append("\t");
            sb.append("\n");
        }
        sb.append("\n");
    }
}
