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
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Unmodifiable file information.
 */
public final class IgfsFileInfo implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** ID for the root directory. */
    public static final IgniteUuid ROOT_ID = new IgniteUuid(new UUID(0, 0), 0);

    /** ID of the trash directory. */
    public static final IgniteUuid TRASH_ID = new IgniteUuid(new UUID(0, 1), 0);

    /** Info ID. */
    private IgniteUuid id;

    /** File length in bytes. */
    private long len;

    /** File block size, {@code zero} for directories. */
    private int blockSize;

    /** File properties. */
    private Map<String, String> props;

    /** File lock ID. */
    private IgniteUuid lockId;

    /** Affinity key used for single-node file collocation. */
    private IgniteUuid affKey;

    /** File affinity map. */
    private IgfsFileMap fileMap;

    /** Last access time. Modified on-demand. */
    private long accessTime;

    /** Last modification time. */
    private long modificationTime;

    /** Directory listing. */
    @GridToStringInclude
    private Map<String, IgfsListingEntry> listing;

    /** Whether data blocks of this entry should never be excluded. */
    private boolean evictExclude;

    /**
     * Original file path. This is a helper field used only in some
     * operations like delete.
     */
    private IgfsPath path;

    /**
     * {@link Externalizable} support.
     */
    public IgfsFileInfo() {
        this(ROOT_ID);
    }

    /**
     * Constructs directory file info with the given ID.
     *
     * @param id ID.
     */
    IgfsFileInfo(IgniteUuid id) {
        this(true, id, 0, 0, null, null, null, null, false, System.currentTimeMillis(), false);
    }

    /**
     * Constructs directory or file info with {@link org.apache.ignite.configuration.FileSystemConfiguration#DFLT_BLOCK_SIZE default} block size.
     *
     * @param isDir Constructs directory info if {@code true} or file info if {@code false}.
     * @param props Meta properties to set.
     */
    public IgfsFileInfo(boolean isDir, @Nullable Map<String, String> props) {
        this(isDir, null, isDir ? 0 : FileSystemConfiguration.DFLT_BLOCK_SIZE, 0, null, null, props, null, false,
            System.currentTimeMillis(), false);
    }

    /**
     * Consturcts directory with random ID and provided listing.
     *
     * @param listing Listing.
     */
    IgfsFileInfo(Map<String, IgfsListingEntry> listing) {
        this(true, null, 0, 0, null, listing, null, null, false, System.currentTimeMillis(), false);
    }

    /**
     * Constructs file info.
     *
     * @param blockSize Block size.
     * @param affKey Affinity key.
     * @param evictExclude Eviction exclude flag.
     * @param props File properties.
     */
    IgfsFileInfo(int blockSize, @Nullable IgniteUuid affKey, boolean evictExclude,
        @Nullable Map<String, String> props) {
        this(false, null, blockSize, 0, affKey, null, props, null, true, System.currentTimeMillis(), evictExclude);
    }

    /**
     * Constructs file info.
     *
     * @param blockSize Block size.
     * @param len Length.
     * @param affKey Affinity key.
     * @param lockId Lock ID.
     * @param props Properties.
     * @param evictExclude Evict exclude flag.
     */
    public IgfsFileInfo(int blockSize, long len, @Nullable IgniteUuid affKey, @Nullable IgniteUuid lockId,
        boolean evictExclude, @Nullable Map<String, String> props) {
        this(false, null, blockSize, len, affKey, null, props, lockId, true, System.currentTimeMillis(), evictExclude);
    }

    /**
     * Constructs file information.
     *
     * @param info File information to copy data from.
     * @param len Size of a file.
     */
    IgfsFileInfo(IgfsFileInfo info, long len) {
        this(info.isDirectory(), info.id, info.blockSize, len, info.affKey, info.listing, info.props, info.fileMap(),
            info.lockId, true, info.accessTime, info.modificationTime, info.evictExclude());
    }

    /**
     * Constructs file info.
     *
     * @param info File info.
     * @param accessTime Last access time.
     * @param modificationTime Last modification time.
     */
    IgfsFileInfo(IgfsFileInfo info, long accessTime, long modificationTime) {
        this(info.isDirectory(), info.id, info.blockSize, info.len, info.affKey, info.listing, info.props,
            info.fileMap(), info.lockId, false, accessTime, modificationTime, info.evictExclude());
    }

    /**
     * Constructs file information.
     *
     * @param info File information to copy data from.
     * @param props File properties to set.
     */
    IgfsFileInfo(IgfsFileInfo info, @Nullable Map<String, String> props) {
        this(info.isDirectory(), info.id, info.blockSize, info.len, info.affKey, info.listing, props,
            info.fileMap(), info.lockId, true, info.accessTime, info.modificationTime, info.evictExclude());
    }

    /**
     * Constructs file info.
     *
     * @param blockSize Block size,
     * @param len Size of a file.
     * @param props File properties to set.
     * @param evictExclude Evict exclude flag.
     */
    IgfsFileInfo(int blockSize, long len, boolean evictExclude, @Nullable Map<String, String> props) {
        this(blockSize == 0, // NB The contract is: (blockSize == null) <=> isDirectory()
            null, blockSize, len, null, null, props, null, true, System.currentTimeMillis(), evictExclude);
    }

    /**
     * Constructs file information.
     *
     * @param info File information to copy data from.
     * @param lockId Lock ID.
     * @param modificationTime Last modification time.
     */
    IgfsFileInfo(IgfsFileInfo info, @Nullable IgniteUuid lockId, long modificationTime) {
        this(info.isDirectory(), info.id, info.blockSize, info.len, info.affKey, info.listing, info.props,
            info.fileMap(), lockId, true, info.accessTime, modificationTime, info.evictExclude());
    }

    /**
     * Constructs file info.
     *
     * @param listing New directory listing.
     * @param old Old file info.
     */
    IgfsFileInfo(Map<String, IgfsListingEntry> listing, IgfsFileInfo old) {
        this(old.isDirectory(), old.id, old.blockSize, old.len, old.affKey, listing, old.props, old.fileMap(),
            old.lockId, false, old.accessTime, old.modificationTime, old.evictExclude());
    }

    /**
     * Constructs file info.
     *
     * @param isDir Constructs directory info if {@code true} or file info if {@code false}.
     * @param id ID or {@code null} to generate it automatically.
     * @param blockSize Block size.
     * @param len Size of a file.
     * @param affKey Affinity key for data blocks.
     * @param listing Directory listing.
     * @param props File properties.
     * @param lockId Lock ID.
     * @param cpProps Flag to copy properties map.
     * @param modificationTime Last modification time.
     * @param evictExclude Evict exclude flag.
     */
    private IgfsFileInfo(boolean isDir, @Nullable IgniteUuid id, int blockSize, long len, @Nullable IgniteUuid affKey,
        @Nullable Map<String, IgfsListingEntry> listing, @Nullable Map<String, String> props,
        @Nullable IgniteUuid lockId, boolean cpProps, long modificationTime, boolean evictExclude) {
        this(isDir, id, blockSize, len, affKey, listing, props, null, lockId, cpProps, modificationTime,
            modificationTime, evictExclude);
    }

    /**
     * Constructs file info.
     *
     * @param isDir Constructs directory info if {@code true} or file info if {@code false}.
     * @param id ID or {@code null} to generate it automatically.
     * @param blockSize Block size.
     * @param len Size of a file.
     * @param affKey Affinity key for data blocks.
     * @param listing Directory listing.
     * @param props File properties.
     * @param fileMap File map.
     * @param lockId Lock ID.
     * @param cpProps Flag to copy properties map.
     * @param accessTime Last access time.
     * @param modificationTime Last modification time.
     * @param evictExclude Evict exclude flag.
     */
    private IgfsFileInfo(boolean isDir, @Nullable IgniteUuid id, int blockSize, long len, @Nullable IgniteUuid affKey,
        @Nullable Map<String, IgfsListingEntry> listing, @Nullable Map<String, String> props,
        @Nullable IgfsFileMap fileMap, @Nullable IgniteUuid lockId, boolean cpProps, long accessTime,
        long modificationTime, boolean evictExclude) {
        assert F.isEmpty(listing) || isDir;

        if (isDir) {
            assert len == 0 : "Directory length should be zero: " + len;
            assert blockSize == 0 : "Directory block size should be zero: " + blockSize;
        }
        else {
            assert len >= 0 : "File length cannot be negative: " + len;
            assert blockSize > 0 : "File block size should be positive: " + blockSize;
        }

        this.id = id == null ? IgniteUuid.randomUuid() : id;
        this.len = isDir ? 0 : len;
        this.blockSize = isDir ? 0 : blockSize;
        this.affKey = affKey;
        this.listing = listing;

        if (fileMap == null && !isDir)
            fileMap = new IgfsFileMap();

        this.fileMap = fileMap;
        this.accessTime = accessTime;
        this.modificationTime = modificationTime;

        // Always make a copy of passed properties collection to escape concurrent modifications.
        this.props = props == null || props.isEmpty() ? null :
            cpProps ? new GridLeanMap<>(props) : props;

        if (listing == null && isDir)
            this.listing = Collections.emptyMap();

        this.lockId = lockId;
        this.evictExclude = evictExclude;
    }

    /**
     * A copy constructor, which takes all data from the specified
     * object field-by-field.
     *
     * @param info An object to copy data info.
     */
    public IgfsFileInfo(IgfsFileInfo info) {
        this(info.isDirectory(), info.id, info.blockSize, info.len, info.affKey, info.listing, info.props,
            info.fileMap(), info.lockId, true, info.accessTime, info.modificationTime, info.evictExclude());
    }

    /**
     * Creates a builder for the new instance of file info.
     *
     * @return A builder to construct a new unmodifiable instance
     *         of this class.
     */
    public static Builder builder() {
        return new Builder(new IgfsFileInfo());
    }

    /**
     * Creates a builder for the new instance of file info,
     * based on the specified origin.
     *
     * @param origin An origin for new instance, from which
     *               the data will be copied.
     * @return A builder to construct a new unmodifiable instance
     *         of this class.
     */
    public static Builder builder(IgfsFileInfo origin) {
        return new Builder(new IgfsFileInfo(origin));
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
     * @return {@code True} if this is a file.
     */
    public boolean isFile() {
        return blockSize > 0;
    }

    /**
     * @return {@code True} if this is a directory.
     */
    public boolean isDirectory() {
        return blockSize == 0;
    }

    /**
     * Get file size.
     *
     * @return File size.
     */
    public long length() {
        assert isFile();

        return len;
    }

    /**
     * Get single data block size to store this file.
     *
     * @return Single data block size to store this file.
     */
    public int blockSize() {
        assert isFile();

        return blockSize;
    }

    /**
     * @return Number of data blocks to store this file.
     */
    public long blocksCount() {
        assert isFile();

        return (len + blockSize() - 1) / blockSize();
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
     * @return Directory listing.
     */
    public Map<String, IgfsListingEntry> listing() {
        // Always wrap into unmodifiable map to be able to avoid illegal modifications in order pieces of the code.
        if (isFile())
            return Collections.unmodifiableMap(Collections.<String, IgfsListingEntry>emptyMap());

        assert listing != null;

        return Collections.unmodifiableMap(listing);
    }

    /**
     * @return Affinity key used for single-node file collocation. If {@code null}, usual
     *      mapper procedure is used for block affinity detection.
     */
    @Nullable public IgniteUuid affinityKey() {
        return affKey;
    }

    /**
     * @param affKey Affinity key used for single-node file collocation.
     */
    public void affinityKey(IgniteUuid affKey) {
        this.affKey = affKey;
    }

    /**
     * @return File affinity map.
     */
    public IgfsFileMap fileMap() {
        return fileMap;
    }

    /**
     * @param fileMap File affinity map.
     */
    public void fileMap(IgfsFileMap fileMap) {
        this.fileMap = fileMap;
    }

    /**
     * Get properties of the file.
     *
     * @return Properties of the file.
     */
    public Map<String, String> properties() {
        return props == null || props.isEmpty() ? Collections.<String, String>emptyMap() :
            Collections.unmodifiableMap(props);
    }

    /**
     * Get lock ID.
     *
     * @return Lock ID if file is locked or {@code null} if file is free of locks.
     */
    @Nullable public IgniteUuid lockId() {
        return lockId;
    }

    /**
     * Get evict exclude flag.
     *
     * @return Evict exclude flag.
     */
    public boolean evictExclude() {
        return evictExclude;
    }

    /**
     * @return Original file path. This is a helper field used only in some operations like delete.
     */
    public IgfsPath path() {
        return path;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, id);
        out.writeInt(blockSize);
        out.writeLong(len);
        U.writeStringMap(out, props);
        U.writeGridUuid(out, lockId);
        U.writeGridUuid(out, affKey);
        out.writeObject(listing);
        out.writeObject(fileMap);
        out.writeLong(accessTime);
        out.writeLong(modificationTime);
        out.writeBoolean(evictExclude);
        out.writeObject(path);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = U.readGridUuid(in);
        blockSize = in.readInt();
        len = in.readLong();
        props = U.readStringMap(in);
        lockId = U.readGridUuid(in);
        affKey = U.readGridUuid(in);
        listing = (Map<String, IgfsListingEntry>)in.readObject();
        fileMap = (IgfsFileMap)in.readObject();
        accessTime = in.readLong();
        modificationTime = in.readLong();
        evictExclude = in.readBoolean();
        path = (IgfsPath)in.readObject();
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

    /**
     * Builder for {@link IgfsFileInfo}.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class Builder {
        /** Instance to build. */
        private final IgfsFileInfo info;

        /**
         * Private constructor.
         *
         * @param info Instance to build.
         */
        private Builder(IgfsFileInfo info) {
            this.info = info;
        }

        /**
         * @param path A new path value.
         * @return This builder instance (for chaining).
         */
        public Builder path(IgfsPath path) {
            info.path = path;

            return this;
        }

        /**
         * Finishes instance construction and returns a resulting
         * unmodifiable instance.
         *
         * @return A constructed instance.
         */
        public IgfsFileInfo build() {
            return info;
        }
    }
}