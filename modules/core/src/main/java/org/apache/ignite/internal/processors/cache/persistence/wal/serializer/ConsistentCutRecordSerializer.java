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

package org.apache.ignite.internal.processors.cache.persistence.wal.serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInput;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.putVersion;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.readVersion;

/** */
public class ConsistentCutRecordSerializer {
    /** */
    private static final byte TRUE = 1;

    /** */
    private static final byte FALSE = 0;

    /**
     * Writes {@link ConsistentCutRecord} to given buffer.
     *
     * @param rec Consistent cut record.
     * @param buf Byte buffer.
     */
    public void write(ConsistentCutRecord rec, ByteBuffer buf) {
        buf.putLong(rec.ver());

        buf.put(rec.finish() ? TRUE : FALSE);

        buf.putInt(rec.include().size());

        for (GridCacheVersion tx: rec.include())
            putVersion(buf, tx, false);

        buf.putInt(rec.check().size());

        for (GridCacheVersion tx: rec.check())
            putVersion(buf, tx, false);

    }

    /**
     * Reads {@link TxRecord} from given input.
     *
     * @param in Input
     * @return TxRecord.
     * @throws IOException In case of fail.
     */
    public ConsistentCutRecord read(ByteBufferBackedDataInput in) throws IOException {
        long ver = in.readLong();

        boolean finish = in.readByte() == TRUE;

        int inclSize = in.readInt();

        Set<GridCacheVersion> include = new HashSet<>();

        for (int i = 0; i < inclSize; i++) {
            GridCacheVersion v = readVersion(in, false);

            include.add(v);
        }

        int chkSize = in.readInt();

        Set<GridCacheVersion> check = new HashSet<>();

        for (int i = 0; i < chkSize; i++) {
            GridCacheVersion v = readVersion(in, false);

            check.add(v);
        }

        return new ConsistentCutRecord(ver, include, check, finish);
    }

    /**
     * Returns size of marshalled {@link TxRecord} in bytes.
     *
     * @param rec TxRecord.
     * @return Size of TxRecord in bytes.
     */
    public int size(ConsistentCutRecord rec) {
        int size = 8;  // ver.
        size += 1;  // finish flag.
        size += 4;  // include tx count.
        size += 4;  // check tx count.

        for (GridCacheVersion v: rec.include())
            size += CacheVersionIO.size(v, false);

        for (GridCacheVersion v: rec.check())
            size += CacheVersionIO.size(v, false);

        return size;
    }
}
