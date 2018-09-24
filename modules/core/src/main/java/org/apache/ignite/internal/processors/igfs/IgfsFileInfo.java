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

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Map;

/**
 * IGFS file info.
 */
public final class IgfsFileInfo extends IgfsEntryInfo implements Binarylizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** File length in bytes. */
    private long len;

    /** File block size, {@code zero} for directories. */
    private int blockSize;

    /** File lock ID. */
    private IgniteUuid lockId;

    /** Affinity key used for single-node file collocation. */
    private IgniteUuid affKey;

    /** File affinity map. */
    private IgfsFileMap fileMap;

    /** Whether data blocks of this entry should never be excluded. */
    private boolean evictExclude;

    /**
     * {@link Externalizable} support.
     */
    public IgfsFileInfo() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public IgfsFileInfo length(long len) {
        IgfsFileInfo res = copy();

        res.len = len;

        return res;
    }

    /** {@inheritDoc} */
    @Override public IgfsEntryInfo listing(@Nullable Map<String, IgfsListingEntry> listing) {
        throw new UnsupportedOperationException("listing");
    }

    /** {@inheritDoc} */
    @Override public IgfsFileInfo lock(IgniteUuid lockId) {
        assert lockId != null;
        assert this.lockId == null;

        IgfsFileInfo res = copy();

        res.lockId = lockId;

        return res;
    }

    /** {@inheritDoc} */
    @Override public IgfsFileInfo unlock(long modificationTime) {
        IgfsFileInfo res = copy();

        res.lockId = null;
        res.modificationTime = modificationTime;

        return res;
    }

    /** {@inheritDoc} */
    @Override public IgfsFileInfo fileMap(IgfsFileMap fileMap) {
        IgfsFileInfo res = copy();

        res.fileMap = fileMap;

        return res;
    }

    /** {@inheritDoc} */
    @Override protected IgfsFileInfo copy() {
        return new IgfsFileInfo(id, blockSize, len, affKey, props, fileMap, lockId, accessTime, modificationTime,
            evictExclude);
    }

    /**
     * Constructs file info.
     *
     * @param id ID or {@code null} to generate it automatically.
     * @param blockSize Block size.
     * @param len Size of a file.
     * @param affKey Affinity key for data blocks.
     * @param props File properties.
     * @param fileMap File map.
     * @param lockId Lock ID.
     * @param accessTime Last access time.
     * @param modificationTime Last modification time.
     * @param evictExclude Evict exclude flag.
     */
    IgfsFileInfo(IgniteUuid id, int blockSize, long len, @Nullable IgniteUuid affKey,
        @Nullable Map<String, String> props, @Nullable IgfsFileMap fileMap, @Nullable IgniteUuid lockId,
        long accessTime, long modificationTime, boolean evictExclude) {
        super(id, props, accessTime, modificationTime);

        this.len = len;
        this.blockSize = blockSize;
        this.affKey = affKey;

        if (fileMap == null)
            fileMap = new IgfsFileMap();

        this.fileMap = fileMap;
        this.lockId = lockId;
        this.evictExclude = evictExclude;
    }

    /** {@inheritDoc} */
    @Override public boolean isFile() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long length() {
        return len;
    }

    /** {@inheritDoc} */
    @Override public int blockSize() {
        return blockSize;
    }

    /** {@inheritDoc} */
    @Override public long blocksCount() {
        return (len + blockSize() - 1) / blockSize();
    }

    /** {@inheritDoc} */
    @Override public Map<String, IgfsListingEntry> listing() {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public boolean hasChildren() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean hasChild(String name) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean hasChild(String name, IgniteUuid expId) {
        return false;
    }

    /** {@inheritDoc} */
    @Override @Nullable public IgniteUuid affinityKey() {
        return affKey;
    }

    /** {@inheritDoc} */
    @Override public IgfsFileMap fileMap() {
        return fileMap;
    }

    /** {@inheritDoc} */
    @Override @Nullable public IgniteUuid lockId() {
        return lockId;
    }

    /** {@inheritDoc} */
    @Override public boolean evictExclude() {
        return evictExclude;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeInt(blockSize);
        out.writeLong(len);
        U.writeGridUuid(out, lockId);
        U.writeGridUuid(out, affKey);
        out.writeObject(fileMap);
        out.writeBoolean(evictExclude);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        blockSize = in.readInt();
        len = in.readLong();
        lockId = U.readGridUuid(in);
        affKey = U.readGridUuid(in);
        fileMap = (IgfsFileMap)in.readObject();
        evictExclude = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter out = writer.rawWriter();

        writeBinary(out);

        out.writeInt(blockSize);
        out.writeLong(len);
        BinaryUtils.writeIgniteUuid(out, lockId);
        BinaryUtils.writeIgniteUuid(out, affKey);
        out.writeObject(fileMap);
        out.writeBoolean(evictExclude);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader in = reader.rawReader();

        readBinary(in);

        blockSize = in.readInt();
        len = in.readLong();
        lockId = BinaryUtils.readIgniteUuid(in);
        affKey = BinaryUtils.readIgniteUuid(in);
        fileMap = in.readObject();
        evictExclude = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id.hashCode() ^ blockSize ^ (int)(len ^ (len >>> 32)) ^ (props == null ? 0 : props.hashCode()) ^
            (lockId == null ? 0 : lockId.hashCode());
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        IgfsFileInfo that = (IgfsFileInfo)obj;

        return id.equals(that.id) && blockSize == that.blockSize && len == that.len && F.eq(affKey, that.affKey) &&
            F.eq(props, that.props) && F.eq(lockId, that.lockId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsFileInfo.class, this);
    }
}