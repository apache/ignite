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
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.processors.igfs.IgfsEntryInfo;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

/**
 * File create processor.
 */
public class IgfsMetaFileCreateProcessor implements EntryProcessor<IgniteUuid, IgfsEntryInfo, IgfsEntryInfo>,
    Externalizable, Binarylizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Create time. */
    private long accessTime;

    /** Modification time. */
    private long modificationTime;

    /** Properties. */
    private Map<String, String> props;

    /** Block size. */
    private int blockSize;

    /** Affintiy key. */
    private IgniteUuid affKey;

    /** Lcok ID. */
    private IgniteUuid lockId;

    /** Evict exclude flag. */
    private boolean evictExclude;

    /** File length. */
    private long len;

    /**
     * Constructor.
     */
    public IgfsMetaFileCreateProcessor() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param accessTime Access time.
     * @param modificationTime Modification time.
     * @param props Properties.
     * @param blockSize Block size.
     * @param affKey Affinity key.
     * @param lockId Lock ID.
     * @param evictExclude Evict exclude flag.
     * @param len File length.
     */
    public IgfsMetaFileCreateProcessor(long accessTime, long modificationTime, Map<String, String> props,
        int blockSize, @Nullable IgniteUuid affKey, IgniteUuid lockId, boolean evictExclude, long len) {
        this.accessTime = accessTime;
        this.modificationTime = modificationTime;
        this.props = props;
        this.blockSize = blockSize;
        this.affKey = affKey;
        this.lockId = lockId;
        this.evictExclude = evictExclude;
        this.len = len;
    }

    /** {@inheritDoc} */
    @Override public IgfsEntryInfo process(MutableEntry<IgniteUuid, IgfsEntryInfo> entry, Object... args)
        throws EntryProcessorException {
        IgfsEntryInfo info = IgfsUtils.createFile(
            entry.getKey(),
            blockSize,
            len,
            affKey,
            lockId,
            evictExclude,
            props,
            accessTime,
            modificationTime
        );

        entry.setValue(info);

        return info;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(accessTime);
        out.writeLong(modificationTime);

        IgfsUtils.writeProperties(out, props);

        out.writeInt(blockSize);
        U.writeGridUuid(out, affKey);
        U.writeGridUuid(out, lockId);
        out.writeBoolean(evictExclude);

        out.writeLong(len);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        accessTime = in.readLong();
        modificationTime = in.readLong();

        props = IgfsUtils.readProperties(in);

        blockSize = in.readInt();
        affKey = U.readGridUuid(in);
        lockId = U.readGridUuid(in);
        evictExclude = in.readBoolean();

        len = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter out = writer.rawWriter();

        out.writeLong(accessTime);
        out.writeLong(modificationTime);

        IgfsUtils.writeProperties(out, props);

        out.writeInt(blockSize);
        BinaryUtils.writeIgniteUuid(out, affKey);
        BinaryUtils.writeIgniteUuid(out, lockId);
        out.writeBoolean(evictExclude);

        out.writeLong(len);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader in = reader.rawReader();

        accessTime = in.readLong();
        modificationTime = in.readLong();

        props = IgfsUtils.readProperties(in);

        blockSize = in.readInt();
        affKey = BinaryUtils.readIgniteUuid(in);
        lockId = BinaryUtils.readIgniteUuid(in);
        evictExclude = in.readBoolean();

        len = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsMetaFileCreateProcessor.class, this);
    }
}
