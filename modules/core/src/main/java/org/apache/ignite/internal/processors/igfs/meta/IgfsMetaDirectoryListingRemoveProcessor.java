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
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.processors.igfs.IgfsEntryInfo;
import org.apache.ignite.internal.processors.igfs.IgfsListingEntry;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

/**
 * Remove entry from directory listing.
 */
public class IgfsMetaDirectoryListingRemoveProcessor implements EntryProcessor<IgniteUuid, IgfsEntryInfo, Void>,
    Externalizable, Binarylizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** File name. */
    private String fileName;

    /** Expected ID. */
    private IgniteUuid fileId;

    /**
     * Default constructor.
     */
    public IgfsMetaDirectoryListingRemoveProcessor() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param fileName File name.
     * @param fileId File ID.
     */
    public IgfsMetaDirectoryListingRemoveProcessor(String fileName, IgniteUuid fileId) {
        this.fileName = fileName;
        this.fileId = fileId;
    }

    /** {@inheritDoc} */
    @Override public Void process(MutableEntry<IgniteUuid, IgfsEntryInfo> e, Object... args)
        throws EntryProcessorException {
        IgfsEntryInfo fileInfo = e.getValue();

        assert fileInfo != null;
        assert fileInfo.isDirectory();

        Map<String, IgfsListingEntry> listing = new HashMap<>(fileInfo.listing());

        IgfsListingEntry oldEntry = listing.get(fileName);

        if (oldEntry == null || !oldEntry.fileId().equals(fileId))
            throw new IgniteException("Directory listing doesn't contain expected file" +
                " [listing=" + listing + ", fileName=" + fileName + "]");

        // Modify listing in-place.
        listing.remove(fileName);

        e.setValue(fileInfo.listing(listing));

        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, fileName);
        U.writeGridUuid(out, fileId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        fileName = U.readString(in);
        fileId = U.readGridUuid(in);
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter out = writer.rawWriter();

        out.writeString(fileName);
        BinaryUtils.writeIgniteUuid(out, fileId);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader in = reader.rawReader();

        fileName = in.readString();
        fileId = BinaryUtils.readIgniteUuid(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsMetaDirectoryListingRemoveProcessor.class, this);
    }
}
