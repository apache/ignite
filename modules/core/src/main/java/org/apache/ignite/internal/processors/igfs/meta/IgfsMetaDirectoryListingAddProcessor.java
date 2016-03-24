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

package org.apache.ignite.internal.processors.igfs.meta;

import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.processors.igfs.IgfsEntryInfo;
import org.apache.ignite.internal.processors.igfs.IgfsListingEntry;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

/**
 * Update directory listing closure.
 */
public final class IgfsMetaDirectoryListingAddProcessor implements EntryProcessor<IgniteUuid, IgfsEntryInfo, Void>,
    Externalizable, Binarylizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** File name to add into parent listing. */
    private String fileName;

    /** File ID.*/
    private IgfsListingEntry entry;

    /**
     * Empty constructor required for {@link Externalizable}.
     *
     */
    public IgfsMetaDirectoryListingAddProcessor() {
        // No-op.
    }

    /**
     * Constructs update directory listing closure.
     *
     * @param fileName File name to add into parent listing.
     * @param entry Listing entry to add or remove.
     */
    public IgfsMetaDirectoryListingAddProcessor(String fileName, IgfsListingEntry entry) {
        assert fileName != null;
        assert entry != null;

        this.fileName = fileName;
        this.entry = entry;
    }

    /** {@inheritDoc} */
    @Override public Void process(MutableEntry<IgniteUuid, IgfsEntryInfo> e, Object... args) {
        IgfsEntryInfo fileInfo = e.getValue();

        assert fileInfo.isDirectory();

        Map<String, IgfsListingEntry> listing = new HashMap<>(fileInfo.listing());

        // Modify listing in-place.
        IgfsListingEntry oldEntry = listing.put(fileName, entry);

        if (oldEntry != null && !oldEntry.fileId().equals(entry.fileId()))
            throw new IgniteException("Directory listing contains unexpected file" +
                " [listing=" + listing + ", fileName=" + fileName + ", entry=" + entry +
                ", oldEntry=" + oldEntry + ']');

        e.setValue(fileInfo.listing(listing));

        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, fileName);

        IgfsUtils.writeListingEntry(out, entry);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        fileName = U.readString(in);

        entry = IgfsUtils.readListingEntry(in);
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter out = writer.rawWriter();

        out.writeString(fileName);

        IgfsUtils.writeListingEntry(out, entry);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader in = reader.rawReader();

        fileName = in.readString();

        entry = IgfsUtils.readListingEntry(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsMetaDirectoryListingAddProcessor.class, this);
    }
}
