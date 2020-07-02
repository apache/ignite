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

/**
 * Statistic for overall WAL file
 */
public class WalStat {
    /** Display max: top lines before merging other values */
    static final int DISPLAY_MAX = 80;

    /** Tx statistics. */
    private TxWalStat txStat = new TxWalStat();

    /** Segments source: work/archive  -> records loaded count & sizes. */
    private final Map<String, RecordSizeCountStat> segmentsFolder = new TreeMap<>();

    /** Segments index -> records loaded count & sizes. */
    private final Map<String, RecordSizeCountStat> segmentsIndexes = new TreeMap<>();

    /** Record type -> its count & sizes. */
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
     * @param key key (parameter) value.
     * @param record record corresponding to {@code key}.
     * @param map map to save statistics.
     * @param <K> key type.
     */
    private static <K> void incrementStat(K key, WALRecord record, Map<K, RecordSizeCountStat> map) {
        incrementStat(key, map, record.size());
    }

    /**
     * @param key key (parameter) value.
     * @param map  to save statistics.
     * @param size record size for record corresponding to {@code key}. Negative value of size means size is unknown for current row.
     * @param <K>  key type.
     */
    private static <K> void incrementStat(K key, Map<K, RecordSizeCountStat> map, int size) {
        final RecordSizeCountStat val = map.get(key);
        final RecordSizeCountStat recordStat = val == null ? new RecordSizeCountStat() : val;
        recordStat.occurrence(size);
        map.put(key, recordStat);
    }

    /**
     * Handles WAL record.
     *
     * @param record record to handle.
     * @param walPointer pointer, used to extract segment index.
     * @param workDir true for work, false for archive folder.
     */
    void registerRecord(WALRecord record, WALPointer walPointer, boolean workDir) {
        WALRecord.RecordType type = record.type();

        if (type == WALRecord.RecordType.PAGE_RECORD)
            registerPageSnapshot((PageSnapshot)record);
        else if (type == WALRecord.RecordType.DATA_RECORD || type == WALRecord.RecordType.MVCC_DATA_RECORD)
            registerDataRecord((DataRecord)record);
        else if (type == WALRecord.RecordType.TX_RECORD || type == WALRecord.RecordType.MVCC_TX_RECORD)
            registerTxRecord((TxRecord)record);

        incrementStat(type.toString(), record, recTypeSizes);

        if (walPointer instanceof FileWALPointer) {
            final FileWALPointer fPtr = (FileWALPointer)walPointer;

            incrementStat(Long.toString(fPtr.index()), record, segmentsIndexes);
            incrementStat(workDir ? "work" : "archive", record, segmentsFolder);
        }
    }

    /**
     * @param txRecord TX record to handle.
     */
    private void registerTxRecord(TxRecord txRecord) {
        final TransactionState state = txRecord.state();

        incrementStat(state.toString(), txRecord, txRecordAct);

        int totalNodes = 0;
        final Map<Short, Collection<Short>> map = txRecord.participatingNodes();

        if (map != null) {
            incrementStat(map.size(), txRecord, txRecordPrimNodesCnt);

            final HashSet<Object> set = new HashSet<>(150);

            for (Map.Entry<Short, Collection<Short>> next : map.entrySet()) {
                set.add(next.getKey());
                set.addAll(next.getValue());
            }

            totalNodes = set.size();

            incrementStat(totalNodes, txRecord, txRecordNodesCnt);
        }

        final GridCacheVersion ver = txRecord.nearXidVersion();
        if (ver != null) {
            switch (state) {
                case PREPARING:
                case PREPARED:
                    txStat.onTxPrepareStart(ver, map != null ? map.size() : 0, totalNodes);
                    break;
                case COMMITTED:
                    txStat.onTxEnd(ver, true);
                    break;
                default:
                    txStat.onTxEnd(ver, false);
            }
        }
    }

    /**
     * @param record page snapshot record to handle.
     */
    private void registerPageSnapshot(PageSnapshot record) {
        FullPageId fullPageId = record.fullPageId();
        long pageId = fullPageId.pageId();

        incrementStat(getPageType(record), record, pageSnapshotTypes);

        final int idx = PageIdUtils.pageIndex(pageId);
        final String idxAsStr = idx <= 100 ? Integer.toString(idx) : ">100";

        incrementStat(idxAsStr, record, pageSnapshotIndexes);
        incrementStat(fullPageId.groupId(), record, pageSnapshotCacheGrp);
        incrementStat(PageIdUtils.partId(pageId), record, pageSnapshotPartId);
    }

    /**
     * @param record data record to handle.
     */
    private void registerDataRecord(DataRecord record) {
        final List<DataEntry> dataEntries = record.writeEntries();
        if (!dataEntries.isEmpty()) {
            boolean underTx = false;
            for (DataEntry next : dataEntries) {
                final int size = dataEntries.size() > 1 ? -1 : record.size();
                incrementStat(next.op().toString(), dataEntryOperation, size);

                incrementStat(next.cacheId(), dataEntryCacheId, size);

                txStat.onDataEntry(next);

                underTx |= next.nearXidVersion() != null;
            }

            incrementStat(underTx, record, dataRecordUnderTx);
        }

        incrementStat(dataEntries.size(), record, dataRecordEntriesCnt);
    }

    /**
     * @param record page snapshot record.
     * @return string identifier of page (IO) type.
     */
    private static String getPageType(PageSnapshot record) {
        byte[] pageData = record.pageData();
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
            GridUnsafe.cleanDirectBuffer(buf);
        }

        return "";
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        final StringBuilder sb = new StringBuilder();

        printSizeCountMap(sb, "WAL Segments: Source folder", segmentsFolder);
        printSizeCountMap(sb, "WAL Segments: File index", segmentsIndexes);

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
     * @param sb buffer.
     * @param mapName display name of map.
     * @param map data.
     */
    private void printSizeCountMap(StringBuilder sb, String mapName, Map<?, RecordSizeCountStat> map) {
        sb.append(mapName).append(": \n");
        sb.append("key\tsize\tcount\tavg.size");
        sb.append("\n");

        final List<? extends Map.Entry<?, RecordSizeCountStat>> entries = new ArrayList<>(map.entrySet());

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
