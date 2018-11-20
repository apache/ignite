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
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
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
import java.util.HashMap;
import java.util.Map;

/**
 * IGFS directory info.
 */
public class IgfsDirectoryInfo extends IgfsEntryInfo implements Binarylizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Directory listing. */
    @GridToStringInclude
    private Map<String, IgfsListingEntry> listing;

    /**
     * {@link Externalizable} support.
     */
    public IgfsDirectoryInfo() {
        // No-op.
    }

    /**
     * Update length.
     *
     * @param len New length.
     * @return Updated file info.
     */
    @Override public IgfsEntryInfo length(long len) {
        throw new UnsupportedOperationException("length");
    }

    /** {@inheritDoc} */
    @Override public IgfsDirectoryInfo listing(@Nullable Map<String, IgfsListingEntry> listing) {
        IgfsDirectoryInfo res = copy();

        res.listing = listing;

        return res;
    }

    /** {@inheritDoc} */
    @Override public IgfsEntryInfo lock(IgniteUuid lockId) {
        throw new UnsupportedOperationException("lock");
    }

    /** {@inheritDoc} */
    @Override public IgfsEntryInfo unlock(long modificationTime) {
        throw new UnsupportedOperationException("unlock");
    }

    /** {@inheritDoc} */
    @Override public IgfsEntryInfo fileMap(IgfsFileMap fileMap) {
        throw new UnsupportedOperationException("fileMap");
    }

    /**
     * Constructs file info.
     *
     * @param id ID or {@code null} to generate it automatically.
     * @param listing Directory listing.
     * @param props File properties.
     * @param accessTime Last access time.
     * @param modificationTime Last modification time.
     */
    IgfsDirectoryInfo(IgniteUuid id, @Nullable Map<String, IgfsListingEntry> listing,
        @Nullable Map<String, String> props, long accessTime, long modificationTime) {
        super(id, props, accessTime, modificationTime);

        this.listing = listing;
    }

    /** {@inheritDoc} */
    @Override protected IgfsDirectoryInfo copy() {
        return new IgfsDirectoryInfo(id, listing, props, accessTime, modificationTime);
    }

    /** {@inheritDoc} */
    @Override public boolean isFile() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public long length() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int blockSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long blocksCount() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public Map<String, IgfsListingEntry> listing() {
        return listing != null ? listing : Collections.<String, IgfsListingEntry>emptyMap();
    }

    /** {@inheritDoc} */
    @Override public boolean hasChildren() {
        return !F.isEmpty(listing);
    }

    /** {@inheritDoc} */
    @Override public boolean hasChild(String name) {
        return listing != null && listing.containsKey(name);
    }

    /** {@inheritDoc} */
    @Override public boolean hasChild(String name, IgniteUuid expId) {
        if (listing != null) {
            IgfsListingEntry entry = listing.get(name);

            if (entry != null)
                return F.eq(expId, entry.fileId());
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override @Nullable public IgniteUuid affinityKey() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgfsFileMap fileMap() {
        return null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public IgniteUuid lockId() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean evictExclude() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        if (listing != null) {
            out.writeBoolean(true);

            out.writeInt(listing.size());

            for (Map.Entry<String, IgfsListingEntry> entry : listing.entrySet()) {
                U.writeString(out, entry.getKey());

                IgfsUtils.writeListingEntry(out, entry.getValue());
            }
        }
        else
            out.writeBoolean(false);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        if (in.readBoolean()) {
            int listingSize = in.readInt();

            listing = new HashMap<>(listingSize);

            for (int i = 0; i < listingSize; i++) {
                String key = U.readString(in);

                IgfsListingEntry val = IgfsUtils.readListingEntry(in);

                listing.put(key, val);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter out = writer.rawWriter();

        writeBinary(out);

        if (listing != null) {
            out.writeBoolean(true);

            out.writeInt(listing.size());

            for (Map.Entry<String, IgfsListingEntry> entry : listing.entrySet()) {
                out.writeString(entry.getKey());

                IgfsUtils.writeListingEntry(out, entry.getValue());
            }
        }
        else
            out.writeBoolean(false);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader in = reader.rawReader();

        readBinary(in);

        if (in.readBoolean()) {
            int listingSize = in.readInt();

            listing = new HashMap<>(listingSize);

            for (int i = 0; i < listingSize; i++) {
                String key = in.readString();

                IgfsListingEntry val = IgfsUtils.readListingEntry(in);

                listing.put(key, val);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id.hashCode() ^ (props == null ? 0 : props.hashCode());
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        IgfsDirectoryInfo that = (IgfsDirectoryInfo)obj;

        return id.equals(that.id) && F.eq(props, that.props);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsDirectoryInfo.class, this);
    }
}
