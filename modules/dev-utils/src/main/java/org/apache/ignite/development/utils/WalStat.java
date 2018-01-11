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
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.GridUnsafe;
import sun.nio.ch.DirectBuffer;

/**
 * Statistic for overall WAL file
 */
public class WalStat {
    /** Rec type sizes. */
    private final Map<String, RecordSizeCountStat> recTypeSizes = new TreeMap<>();

    /** Data Record: operation performed */
    private final Map<String, RecordSizeCountStat> dataRecordOperation = new TreeMap<>();

    /** Tx Record: action */
    private final Map<String, RecordSizeCountStat> txRecordAct = new TreeMap<>();

    /** Tx Record: participating primary nodes */
    private final Map<Integer, RecordSizeCountStat> txRecordPrimNodesCnt = new TreeMap<>();

    /** Tx Record: participating primary nodes */
    private final Map<Integer, RecordSizeCountStat> txRecordNodesCnt = new TreeMap<>();

    /** Page snapshot types. */
    private final Map<String, RecordSizeCountStat> pageSnapshotTypes = new TreeMap<>();
    /** Page snapshot partition. */
    private final Map<Integer, RecordSizeCountStat> pageSnapshotPart = new TreeMap<>();
    /** Page snapshot indexes. */
    private final Map<Integer, RecordSizeCountStat> pageSnapshotIndexes = new TreeMap<>();
    /** Page snapshot cache groups. */
    private final Map<Integer, RecordSizeCountStat> pageSnapshotCacheGroups = new TreeMap<>();

    /**
     * @param key
     * @param record
     */
    private void registerRecSize(String key, WALRecord record) {
        computeStatIfAbsent(key, record, recTypeSizes);
    }

    /**
     * @param key
     * @param record
     * @param map
     * @param <K>
     */
    private <K> void computeStatIfAbsent(K key, WALRecord record, Map<K, RecordSizeCountStat> map) {
        final RecordSizeCountStat val = map.get(key);
        final RecordSizeCountStat recordStat = val == null ? new RecordSizeCountStat() : val;
        recordStat.occurrence(record.size());
        map.put(key, recordStat);
    }

    /**
     * @param type
     * @param record
     */
    void registerRecord(WALRecord.RecordType type, WALRecord record) {
        String key = type.toString();
        if (type == WALRecord.RecordType.PAGE_RECORD)
            registerPageSnapshot((PageSnapshot)record);
        else if (type == WALRecord.RecordType.DATA_RECORD)
            registerDataRecord((DataRecord)record);
        else if (type == WALRecord.RecordType.TX_RECORD)
            registerTxRecord((TxRecord)record);

        registerRecSize(key, record);
    }

    /**
     * @param txRecord
     */
    private void registerTxRecord(TxRecord txRecord) {
        computeStatIfAbsent(txRecord.state().toString(), txRecord, txRecordAct);

        final Map<Short, Collection<Short>> map = txRecord.participatingNodes();
        if (map != null) {
            computeStatIfAbsent(map.keySet().size(), txRecord, txRecordPrimNodesCnt);

            final HashSet<Object> set = new HashSet<>();

            for (Map.Entry<Short, Collection<Short>> next : map.entrySet()) {
                set.add(next.getKey());
                set.addAll(next.getValue());
            }

            computeStatIfAbsent(set.size(), txRecord, txRecordNodesCnt);
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
        computeStatIfAbsent(idx, record, pageSnapshotIndexes);
        computeStatIfAbsent(fullPageId.groupId(), record, pageSnapshotCacheGroups);
    }

    /**
     * @param record
     */
    private void registerDataRecord(DataRecord record) {
        if (!record.writeEntries().isEmpty())
            computeStatIfAbsent(record.writeEntries().get(0).op().toString(), record, dataRecordOperation);
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

        printSizeCountMap(sb, "Record type", recTypeSizes);

        printSizeCountMap(sb, "Tx Record: Action", txRecordAct);
        printSizeCountMap(sb, "Tx Record: Primary nodes count", txRecordPrimNodesCnt);
        printSizeCountMap(sb, "Tx Record: Nodes count", txRecordNodesCnt);

        printSizeCountMap(sb, "Data Record Operations", dataRecordOperation);

        printSizeCountMap(sb, "Page Snapshot Page Types", pageSnapshotTypes);
        printSizeCountMap(sb, "Page Snapshot Indexes", pageSnapshotIndexes);
        printSizeCountMap(sb, "Page Snapshot Cache Groups", pageSnapshotCacheGroups);

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
        for (Map.Entry<?, RecordSizeCountStat> next : map.entrySet()) {
            sb.append(next.getKey()).append("\t").append(next.getValue()).append("\t");
            sb.append("\n");
        }
        sb.append("\n");
    }
}
