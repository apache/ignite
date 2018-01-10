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
import java.util.Map;
import java.util.TreeMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.GridUnsafe;
import sun.nio.ch.DirectBuffer;

/**
 * Statistic for overall WAL file
 */
public class WalStat {
    /** Rec type sizes. */
    final Map<String, RecordStat> recTypeSizes = new TreeMap<>();
    /** Page snapshot types. */
    final Map<String, RecordStat> pageSnapshotTypes = new TreeMap<>();
    /** Page snapshot partition. */
    final Map<Integer, RecordStat> pageSnapshotPart = new TreeMap<>();
    /** Page snapshot indexes. */
    final Map<Integer, RecordStat> pageSnapshotIndexes = new TreeMap<>();
    /** Page snapshot cache groups. */
    final Map<Integer, RecordStat> pageSnapshotCacheGroups = new TreeMap<>();

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
    private <K> void computeStatIfAbsent(K key, WALRecord record, Map<K, RecordStat> map) {
        final RecordStat val = map.get(key);
        final RecordStat recordStat = val == null ? new RecordStat() : val;
        recordStat.occurrence(record.size());
        map.put(key, recordStat);
    }

    /**
     * @param type
     * @param record
     */
    void registerRecord(WALRecord.RecordType type, WALRecord record) {
        String key = type.toString();
        if (type == WALRecord.RecordType.PAGE_RECORD) {
            final PageSnapshot record1 = (PageSnapshot)record;

            registerPageSnapshot(record1);
        }

        registerRecSize(key, record);
    }

    /**
     * @param record
     */
    private void registerPageSnapshot(PageSnapshot record) {
        FullPageId fullPageId = record.fullPageId();
        long pageId = fullPageId.pageId();
        final int idx = PageIdUtils.pageIndex(pageId);

        final String type = getType(record);
        // if (idx == 8 || idx == 9)
        //    System.out.println(idx + ":" + type);
        computeStatIfAbsent(type, record, pageSnapshotTypes);
        computeStatIfAbsent(PageIdUtils.partId(pageId), record, pageSnapshotPart);
        computeStatIfAbsent(idx, record, pageSnapshotIndexes);
        computeStatIfAbsent(fullPageId.groupId(), record, pageSnapshotCacheGroups);
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

        printMap(sb, "Record size in bytes", recTypeSizes);
        printMap(sb, "Page Snapshot Page Types", pageSnapshotTypes);
        printMap(sb, "Page Snapshot Partitions", pageSnapshotPart);
        printMap(sb, "Page Snapshot Indexes", pageSnapshotIndexes);
        printMap(sb, "Page Snapshot Cache Groups", pageSnapshotCacheGroups);

        return sb.toString();
    }

    /**
     * @param sb
     * @param mapName
     * @param map
     */
    private void printMap(StringBuilder sb, String mapName, Map<?, RecordStat> map) {
        sb.append(mapName).append(": \n");
        sb.append("key\tsize\tcount\tavg.size");
        sb.append("\n");
        for (Map.Entry<?, RecordStat> next : map.entrySet()) {
            sb.append(next.getKey()).append("\t").append(next.getValue()).append("\t");
            sb.append("\n");
        }
        sb.append("\n");
    }
}
