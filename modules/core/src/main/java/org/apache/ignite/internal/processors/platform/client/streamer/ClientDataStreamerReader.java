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

package org.apache.ignite.internal.processors.platform.client.streamer;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerEntry;

/**
 * Data streamer deserialization helpers.
 */
class ClientDataStreamerReader {
    /**
     * Reads an entry.
     *
     * @param reader Data reader.
     * @return Streamer entry.
     */
    public static Collection<DataStreamerEntry> read(BinaryReaderExImpl reader) {
        int entriesCnt = reader.readInt();

        if (entriesCnt == 0)
            return null;

        Collection<DataStreamerEntry> entries = new ArrayList<>(entriesCnt);

        for (int i = 0; i < entriesCnt; i++) {
            entries.add(new DataStreamerEntry(readCacheObject(reader, true),
                    readCacheObject(reader, false)));
        }

        return entries;
    }

    /**
     * Read cache object from the stream as raw bytes to avoid marshalling.
     */
    private static <T extends CacheObject> T readCacheObject(BinaryReaderExImpl reader, boolean isKey) {
        BinaryInputStream in = reader.in();

        int pos0 = in.position();

        Object obj = reader.readObjectDetached();

        if (obj == null)
            return null;

        int pos1 = in.position();

        in.position(pos0);

        byte[] objBytes = in.readByteArray(pos1 - pos0);

        return isKey ? (T) new KeyCacheObjectImpl(obj, objBytes, -1) : (T) new CacheObjectImpl(obj, objBytes);
    }
}
