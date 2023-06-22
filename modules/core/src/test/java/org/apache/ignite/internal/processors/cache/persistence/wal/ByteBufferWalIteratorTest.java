/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactoryImpl;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.wal.record.RecordUtils;
import org.junit.Test;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_FORWARD_PAGE_SPLIT;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_META_PAGE_INIT_ROOT_V3;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_PAGE_INNER_REPLACE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_PAGE_MERGE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.PARTITION_META_PAGE_DELTA_RECORD_V4;

/**
 *
 */
public class ByteBufferWalIteratorTest extends GridCommonAbstractTest {

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** */
    private static ByteBuffer readFile(File file) throws IOException {
        int byteCount = (int)file.length();

        FileInputStream fileInputStream = new FileInputStream(file);

        final byte[] bytes = new byte[byteCount];

        int l = fileInputStream.read(bytes);

        return ByteBuffer.wrap(bytes, 0, l);
    }

    /** */
    private static boolean dataEntriesEqual(DataEntry x, DataEntry y) {
        if (x == y)
            return true;

        if (x == null || y == null)
            return false;

        return x.cacheId() == y.cacheId()
            && x.op() == y.op()
            && Objects.equals(x.key(), y.key());
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(new CacheConfiguration(CACHE_NAME));

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        return cfg;
    }

    /** */
    @Test
    public void test() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().state(ClusterState.ACTIVE);

        GridCacheSharedContext<Object, Object> sharedCtx = ig.context().cache().context();

        GridCacheContext<Object, Object> cctx = sharedCtx.cache().cache(CACHE_NAME).context();

        IgniteWriteAheadLogManager wal = sharedCtx.wal();

        RecordSerializerFactory serializerFactory = new RecordSerializerFactoryImpl(sharedCtx);

        RecordSerializer serializer = serializerFactory.createSerializer(RecordSerializerFactory.LATEST_SERIALIZER_VERSION);

        List<WALRecord.RecordType> skipTypes = Arrays.asList(
            BTREE_PAGE_INNER_REPLACE,
            BTREE_FORWARD_PAGE_SPLIT,
            BTREE_PAGE_MERGE,
            BTREE_META_PAGE_INIT_ROOT_V3,
            PARTITION_META_PAGE_DELTA_RECORD_V4
        );

        ByteBuffer byteBuffer = ByteBuffer.allocate(1024 * 1024).order(ByteOrder.nativeOrder());

        List<WALRecord> physicalRecords = Arrays.stream(WALRecord.RecordType.values())
            .filter(t -> t.purpose() == WALRecord.RecordPurpose.PHYSICAL && !skipTypes.contains(t))
            .map(RecordUtils::buildWalRecord)
            .collect(Collectors.toList());

        final int cnt = physicalRecords.size();

        List<DataEntry> entries = new ArrayList<>(cnt);

        for (int i = 0; i < cnt; i++) {
            GridCacheOperation op = i % 2 == 0 ? GridCacheOperation.UPDATE : GridCacheOperation.DELETE;

            KeyCacheObject key = cctx.toCacheKeyObject(i);

            CacheObject val = null;

            if (op != GridCacheOperation.DELETE)
                val = cctx.toCacheObject("value-" + i);

            entries.add(
                new DataEntry(cctx.cacheId(), key, val, op, null, cctx.cache().nextVersion(),
                    0L,
                    cctx.affinity().partition(i), i, DataEntry.EMPTY_FLAGS));
        }

        for (int i = 0; i < entries.size(); i++) {
            DataRecord dataRecord = new DataRecord(entries.get(i));

            // make sure walpointer is set.
            wal.log(dataRecord);

            serializer.writeRecord(dataRecord, byteBuffer);

            WALRecord physicalRecord = physicalRecords.get(i);

            wal.log(physicalRecord);

            serializer.writeRecord(physicalRecord, byteBuffer);
        }

        byteBuffer.flip();

        WALIterator walIterator = new ByteBufferWalIterator(log, sharedCtx, byteBuffer);

        Iterator<DataEntry> dataEntriesIterator = entries.iterator();

        while (walIterator.hasNext()) {
            assertTrue(dataEntriesIterator.hasNext());

            WALRecord record = walIterator.next().get2();

            assertTrue(record instanceof DataRecord);

            DataEntry dataEntry = dataEntriesIterator.next();

            assertTrue(dataEntriesEqual(
                ((DataRecord)record).get(0),
                dataEntry));
        }

        assertFalse(dataEntriesIterator.hasNext());
    }
}
