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

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.internal.binary.BinaryUtils;
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
 * Base IGFS entry.
 */
public abstract class IgfsEntryInfo implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** ID. */
    protected IgniteUuid id;

    /** Properties. */
    protected Map<String, String> props;

    /** Last access time. */
    protected long accessTime;

    /** Last modification time. */
    protected long modificationTime;

    /**
     * Default constructor.
     */
    protected IgfsEntryInfo() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param id ID.
     * @param props Properties.
     * @param accessTime Access time.
     * @param modificationTime Modification time.
     */
    protected IgfsEntryInfo(IgniteUuid id, @Nullable Map<String, String> props, long accessTime,
        long modificationTime) {
        assert id != null;

        this.id = id;
        this.props = props == null || props.isEmpty() ? null : props;
        this.accessTime = accessTime;
        this.modificationTime = modificationTime;
    }

    /**
     * Gets this item ID.
     *
     * @return This item ID.
     */
    public IgniteUuid id() {
        return id;
    }

    /**
     * Get properties of the file.
     *
     * @return Properties of the file.
     */
    public Map<String, String> properties() {
        return props == null ? Collections.<String, String>emptyMap() : Collections.unmodifiableMap(props);
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
     * @return {@code True} if this is a file.
     */
    public abstract boolean isFile();

    /**
     * Update length.
     *
     * @param len New length.
     * @return Updated file info.
     */
    public abstract IgfsEntryInfo length(long len);

    /**
     * Update listing.
     *
     * @param listing Listing.
     * @return Updated file info.
     */
    public abstract IgfsEntryInfo listing(@Nullable Map<String, IgfsListingEntry> listing);

    /**
     * Update properties.
     *
     * @param props Properties.
     * @return Updated file info.
     */
    public IgfsEntryInfo properties(@Nullable Map<String, String> props) {
        IgfsEntryInfo res = copy();

        res.props = props;

        return res;
    }

    /**
     * Update access and modification time.
     *
     * @param accessTime Access time.
     * @param modificationTime Modification time.
     * @return Updated file info.
     */
    public IgfsEntryInfo accessModificationTime(long accessTime, long modificationTime) {
        IgfsEntryInfo res = copy();

        res.accessTime = accessTime;
        res.modificationTime = modificationTime;

        return res;
    }

    /**
     * Lock file.
     *
     * @param lockId Lock ID.
     * @return Update file info.
     */
    public abstract IgfsEntryInfo lock(IgniteUuid lockId);

    /**
     * Unlock file.
     *
     * @param modificationTime Modification time.
     * @return Updated file info.
     */
    public abstract IgfsEntryInfo unlock(long modificationTime);

    /**
     * Update file map.
     *
     * @param fileMap File affinity map.
     * @return Updated file info.
     */
    public abstract IgfsEntryInfo fileMap(IgfsFileMap fileMap);

    /**
     * Copy file info.
     *
     * @return Copy.
     */
    protected abstract IgfsEntryInfo copy();

    /**
     * @return {@code True} if this is a directory.
     */
    public boolean isDirectory() {
        return !isFile();
    }

    /**
     * Get file size.
     *
     * @return File size.
     */
    public abstract long length();

    /**
     * Get single data block size to store this file.
     *
     * @return Single data block size to store this file.
     */
    public abstract int blockSize();

    /**
     * @return Number of data blocks to store this file.
     */
    public abstract long blocksCount();

    /**
     * @return Directory listing.
     */
    public abstract Map<String, IgfsListingEntry> listing();

    /**
     * @return {@code True} if at least one child exists.
     */
    public abstract boolean hasChildren();

    /**
     * @param name Child name.
     * @return {@code True} if child with such name exists.
     */
    public abstract boolean hasChild(String name);

    /**
     * @param name Child name.
     * @param expId Expected child ID.
     * @return {@code True} if child with such name exists.
     */
    public abstract boolean hasChild(String name, IgniteUuid expId);

    /**
    * @return Affinity key used for single-node file collocation. If {@code null}, usual
    *      mapper procedure is used for block affinity detection.
    */
    @Nullable public abstract IgniteUuid affinityKey();

    /**
     * @return File affinity map.
     */
    public abstract IgfsFileMap fileMap();

    /**
     * Get lock ID.
     *
     * @return Lock ID if file is locked or {@code null} if file is free of locks.
     */
    @Nullable public abstract IgniteUuid lockId();

    /**
     * Get evict exclude flag.
     *
     * @return Evict exclude flag.
     */
    public abstract boolean evictExclude();

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, id);

        IgfsUtils.writeProperties(out, props);

        out.writeLong(accessTime);
        out.writeLong(modificationTime);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = U.readGridUuid(in);

        props = IgfsUtils.readProperties(in);

        accessTime = in.readLong();
        modificationTime = in.readLong();
    }

    /**
     * Write binary content.
     *
     * @param out Writer.
     */
    protected void writeBinary(BinaryRawWriter out) {
        BinaryUtils.writeIgniteUuid(out, id);

        IgfsUtils.writeProperties(out, props);

        out.writeLong(accessTime);
        out.writeLong(modificationTime);
    }

    /**
     * Read binary content.
     *
     * @param in Reader.
     */
    protected void readBinary(BinaryRawReader in) {
        id = BinaryUtils.readIgniteUuid(in);

        props = IgfsUtils.readProperties(in);

        accessTime = in.readLong();
        modificationTime = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsEntryInfo.class, this);
    }
}
