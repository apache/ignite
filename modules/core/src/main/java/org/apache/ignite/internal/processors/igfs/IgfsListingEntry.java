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

package org.apache.ignite.internal.processors.igfs;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Directory listing entry.
 */
public class IgfsListingEntry implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** File id. */
    private IgniteUuid fileId;

    /** File affinity key. */
    private IgniteUuid affKey;

    /** Positive block size if file, 0 if directory. */
    private int blockSize;

    /** File length. */
    private long len;

    /** Last access time. */
    private long accessTime;

    /** Last modification time. */
    private long modificationTime;

    /** File properties. */
    private Map<String, String> props;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public IgfsListingEntry() {
        // No-op.
    }

    /**
     * @param fileInfo File info to construct listing entry from.
     */
    public IgfsListingEntry(IgfsFileInfo fileInfo) {
        fileId = fileInfo.id();
        affKey = fileInfo.affinityKey();

        if (fileInfo.isFile()) {
            blockSize = fileInfo.blockSize();
            len = fileInfo.length();
        }

        props = fileInfo.properties();
        accessTime = fileInfo.accessTime();
        modificationTime = fileInfo.modificationTime();
    }

    /**
     * Creates listing entry with updated length.
     *
     * @param entry Entry.
     * @param len New length.
     */
    public IgfsListingEntry(IgfsListingEntry entry, long len, long accessTime, long modificationTime) {
        fileId = entry.fileId;
        affKey = entry.affKey;
        blockSize = entry.blockSize;
        props = entry.props;
        this.accessTime = accessTime;
        this.modificationTime = modificationTime;

        this.len = len;
    }

    /**
     * @return Entry file ID.
     */
    public IgniteUuid fileId() {
        return fileId;
    }

    /**
     * @return File affinity key, if specified.
     */
    public IgniteUuid affinityKey() {
        return affKey;
    }

    /**
     * @return {@code True} if entry represents file.
     */
    public boolean isFile() {
        return blockSize > 0;
    }

    /**
     * @return {@code True} if entry represents directory.
     */
    public boolean isDirectory() {
        return blockSize == 0;
    }

    /**
     * @return Block size.
     */
    public int blockSize() {
        return blockSize;
    }

    /**
     * @return Length.
     */
    public long length() {
        return len;
    }

    /**
     * @return Last access time.
     */
    public long accessTime() {
        return accessTime;
    }

    /**
     * @return Last modification time.
     */
    public long modificationTime() {
        return modificationTime;
    }

    /**
     * @return Properties map.
     */
    public Map<String, String> properties() {
        return props;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, fileId);
        out.writeInt(blockSize);
        out.writeLong(len);
        U.writeStringMap(out, props);
        out.writeLong(accessTime);
        out.writeLong(modificationTime);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        fileId = U.readGridUuid(in);
        blockSize = in.readInt();
        len = in.readLong();
        props = U.readStringMap(in);
        accessTime = in.readLong();
        modificationTime = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IgfsListingEntry)) return false;

        IgfsListingEntry that = (IgfsListingEntry)o;

        return fileId.equals(that.fileId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return fileId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsListingEntry.class, this);
    }
}