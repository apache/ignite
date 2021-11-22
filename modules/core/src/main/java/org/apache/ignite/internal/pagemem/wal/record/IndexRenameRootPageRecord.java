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

package org.apache.ignite.internal.pagemem.wal.record;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.typedef.internal.S;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Logical record for renaming index root pages.
 */
public class IndexRenameRootPageRecord extends WALRecord {
    /** Cache id. */
    private final int cacheId;

    /** Old name of underlying index tree name. */
    private final String oldTreeName;

    /** New name of underlying index tree name. */
    private final String newTreeName;

    /** Number of segments. */
    private final int segments;

    /**
     * Constructor.
     *
     * @param cacheId Cache id.
     * @param oldTreeName Old name of underlying index tree name.
     * @param newTreeName New name of underlying index tree name.
     * @param segments Number of segments.
     */
    public IndexRenameRootPageRecord(int cacheId, String oldTreeName, String newTreeName, int segments) {
        this.cacheId = cacheId;
        this.oldTreeName = oldTreeName;
        this.newTreeName = newTreeName;
        this.segments = segments;
    }

    /**
     * Constructor.
     *
     * @param in Data input.
     * @throws IOException If there are errors while reading the data.
     * @see #writeRecord
     * @see #dataSize
     */
    public IndexRenameRootPageRecord(DataInput in) throws IOException {
        cacheId = in.readInt();
        segments = in.readInt();

        byte[] oldTreeNameBytes = new byte[in.readInt()];
        in.readFully(oldTreeNameBytes);

        oldTreeName = new String(oldTreeNameBytes, UTF_8);

        byte[] newTreeNameBytes = new byte[in.readInt()];
        in.readFully(newTreeNameBytes);

        newTreeName = new String(newTreeNameBytes, UTF_8);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.INDEX_ROOT_PAGE_RENAME_RECORD;
    }

    /**
     * Getting cache id.
     *
     * @return Cache id.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * Getting old name of underlying index tree name.
     *
     * @return Old name of underlying index tree name.
     */
    public String oldTreeName() {
        return oldTreeName;
    }

    /**
     * Getting new name of underlying index tree name.
     *
     * @return New name of underlying index tree name.
     */
    public String newTreeName() {
        return newTreeName;
    }

    /**
     * Getting number of segments.
     *
     * @return Number of segments.
     */
    public int segments() {
        return segments;
    }

    /**
     * Calculating the size of the data.
     *
     * @return Size in bytes.
     * @see #IndexRenameRootPageRecord(DataInput)
     * @see #writeRecord
     */
    public int dataSize() {
        return /* cacheId */4 + /* segments */4 + /* oldTreeNameLen */4 + oldTreeName.getBytes(UTF_8).length +
            /* newTreeNameLen */4 + newTreeName.getBytes(UTF_8).length;
    }

    /**
     * Writing data to the buffer.
     *
     * @param buf Output buffer.
     * @see #IndexRenameRootPageRecord(DataInput)
     * @see #dataSize
     */
    public void writeRecord(ByteBuffer buf) {
        buf.putInt(cacheId);
        buf.putInt(segments);

        byte[] oldTreeNameBytes = oldTreeName.getBytes(UTF_8);
        buf.putInt(oldTreeNameBytes.length);
        buf.put(oldTreeNameBytes);

        byte[] newTreeNameBytes = newTreeName.getBytes(UTF_8);
        buf.putInt(newTreeNameBytes.length);
        buf.put(newTreeNameBytes);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IndexRenameRootPageRecord.class, this);
    }
}
