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

package org.apache.ignite.internal.processors.platform.websession;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Web session lock info.
 */
public class KeyValueDirtyTrackedCollection implements Binarylizable {
    /** Entry order is important. */
    private SortedMap<String, byte[]> entries;

    /** */
    private List<String> removedKeys;

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        assert removedKeys == null;  // Can't write diff.

        BinaryRawWriter raw = writer.rawWriter();

        raw.writeBoolean(true);  // Always full mode.

        raw.writeInt(entries.size());

        for (Map.Entry<String, byte[]> e : entries.entrySet()) {
            raw.writeString(e.getKey());
            raw.writeByteArray(e.getValue());
        }
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader raw = reader.rawReader();

        boolean isDiff = !raw.readBoolean();

        int count = raw.readInt();

        entries = new TreeMap<>();

        for (int i = 0; i < count; i++)
            entries.put(raw.readString(), raw.readByteArray());

        if (isDiff) {
            count = raw.readInt();

            removedKeys = new ArrayList<>(count);

            for (int i = 0; i < count; i++)
                removedKeys.add(raw.readString());
        }
    }

    /**
     * Apply changes from another instance.
     *
     * @param other Items.
     */
    public void applyChanges(KeyValueDirtyTrackedCollection other) {
        assert other != null;

        if (other.removedKeys != null) {
            for (String key : other.removedKeys)
                entries.remove(key);
        }
        else {
            // Not a diff: remove all
            entries.clear();
        }

        for (Map.Entry<String, byte[]> e : other.entries.entrySet())
            entries.put(e.getKey(), e.getValue());
    }
}
