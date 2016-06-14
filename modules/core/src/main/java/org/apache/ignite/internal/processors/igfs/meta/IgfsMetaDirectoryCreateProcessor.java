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
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Map;

/**
 * Directory create processor.
 */
public class IgfsMetaDirectoryCreateProcessor implements EntryProcessor<IgniteUuid, IgfsEntryInfo, IgfsEntryInfo>,
    Externalizable, Binarylizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Access time. */
    private long accessTime;

    /** Modification time. */
    private long modificationTime;

    /** Properties. */
    private Map<String, String> props;

    /** Child name (optional). */
    private String childName;

    /** Child entry (optional. */
    private IgfsListingEntry childEntry;

    /**
     * Constructor.
     */
    public IgfsMetaDirectoryCreateProcessor() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param accessTime Create time.
     * @param modificationTime Modification time.
     * @param props Properties.
     */
    public IgfsMetaDirectoryCreateProcessor(long accessTime, long modificationTime, Map<String, String> props) {
        this(accessTime, modificationTime, props, null, null);
    }

    /**
     * Constructor.
     *
     * @param accessTime Create time.
     * @param modificationTime Modification time.
     * @param props Properties.
     * @param childName Child name.
     * @param childEntry Child entry.
     */
    public IgfsMetaDirectoryCreateProcessor(long accessTime, long modificationTime, Map<String, String> props,
        String childName, IgfsListingEntry childEntry) {
        this.accessTime = accessTime;
        this.modificationTime = modificationTime;
        this.props = props;
        this.childName = childName;
        this.childEntry = childEntry;
    }

    /** {@inheritDoc} */
    @Override public IgfsEntryInfo process(MutableEntry<IgniteUuid, IgfsEntryInfo> entry, Object... args)
        throws EntryProcessorException {

        IgfsEntryInfo info = IgfsUtils.createDirectory(
            entry.getKey(),
            null,
            props,
            accessTime,
            modificationTime
        );

        if (childName != null)
            info = info.listing(Collections.singletonMap(childName, childEntry));

        entry.setValue(info);

        return info;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(accessTime);
        out.writeLong(modificationTime);

        IgfsUtils.writeProperties(out, props);

        U.writeString(out, childName);

        if (childName != null)
            IgfsUtils.writeListingEntry(out, childEntry);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        accessTime = in.readLong();
        modificationTime = in.readLong();

        props = IgfsUtils.readProperties(in);

        childName = U.readString(in);

        if (childName != null)
            childEntry = IgfsUtils.readListingEntry(in);
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter out = writer.rawWriter();

        out.writeLong(accessTime);
        out.writeLong(modificationTime);

        IgfsUtils.writeProperties(out, props);

        out.writeString(childName);

        if (childName != null)
            IgfsUtils.writeListingEntry(out, childEntry);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader in = reader.rawReader();

        accessTime = in.readLong();
        modificationTime = in.readLong();

        props = IgfsUtils.readProperties(in);

        childName = in.readString();

        if (childName != null)
            childEntry = IgfsUtils.readListingEntry(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsMetaDirectoryCreateProcessor.class, this);
    }
}
