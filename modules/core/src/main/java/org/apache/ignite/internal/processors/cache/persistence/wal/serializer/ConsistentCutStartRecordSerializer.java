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
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInput;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.putVersion;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.readVersion;

/** */
public class ConsistentCutStartRecordSerializer {
    /**
     * Writes {@link ConsistentCutStartRecord} to given buffer.
     *
     * @param rec Consistent cut record.
     * @param buf Byte buffer.
     */
    public void write(ConsistentCutStartRecord rec, ByteBuffer buf) {
        buf.putLong(rec.version());

        buf.putInt(rec.include().size());

        for (GridCacheVersion tx: rec.include())
            putVersion(buf, tx, false);
    }

    /**
     * Reads {@link ConsistentCutStartRecord} from given input.
     *
     * @param in Input
     * @return ConsistentCutStartRecord.
     * @throws IOException In case of fail.
     */
    public ConsistentCutStartRecord read(ByteBufferBackedDataInput in) throws IOException {
        long ver = in.readLong();

        int inclSize = in.readInt();

        Set<GridCacheVersion> include = new HashSet<>();

        for (int i = 0; i < inclSize; i++) {
            GridCacheVersion v = readVersion(in, false);

            include.add(v);
        }

        return new ConsistentCutStartRecord(ver, include);
    }

    /**
     * Returns size of marshalled {@link ConsistentCutStartRecord} in bytes.
     *
     * @param rec ConsistentCutStartRecord.
     * @return Size of ConsistentCutStartRecord in bytes.
     */
    public int size(ConsistentCutStartRecord rec) {
        int size = 8;  // ver.
        size += 4;  // include tx count.

        for (GridCacheVersion v: rec.include())
            size += CacheVersionIO.size(v, false);

        return size;
    }
}
