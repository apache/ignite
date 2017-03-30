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
import org.apache.ignite.internal.processors.igfs.IgfsFileAffinityRange;
import org.apache.ignite.internal.processors.igfs.IgfsFileMap;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * File unlock entry processor.
 */
public class IgfsMetaFileUnlockProcessor implements EntryProcessor<IgniteUuid, IgfsEntryInfo, Void>,
    Externalizable, Binarylizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Modification time. */
    private long modificationTime;

    /** Whether to update space. */
    private boolean updateSpace;

    /** Space. */
    private long space;

    /** Affinity range. */
    private IgfsFileAffinityRange affRange;

    /**
     * Default constructor.
     */
    public IgfsMetaFileUnlockProcessor() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param modificationTime Modification time.
     * @param updateSpace Whether to update space.
     * @param space Space.
     * @param affRange Affinity range.
     */
    public IgfsMetaFileUnlockProcessor(long modificationTime, boolean updateSpace, long space,
        @Nullable IgfsFileAffinityRange affRange) {
        this.modificationTime = modificationTime;
        this.updateSpace = updateSpace;
        this.space = space;
        this.affRange = affRange;
    }

    /** {@inheritDoc} */
    @Override public Void process(MutableEntry<IgniteUuid, IgfsEntryInfo> entry, Object... args)
        throws EntryProcessorException {
        IgfsEntryInfo oldInfo = entry.getValue();

        assert oldInfo != null;

        IgfsEntryInfo newInfo = oldInfo.unlock(modificationTime);

        if (updateSpace) {
            IgfsFileMap newMap = new IgfsFileMap(newInfo.fileMap());

            newMap.addRange(affRange);

            newInfo = newInfo.length(newInfo.length() + space).fileMap(newMap);
        }

        entry.setValue(newInfo);

        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(modificationTime);

        if (updateSpace) {
            out.writeBoolean(true);
            out.writeLong(space);
            out.writeObject(affRange);
        }
        else
            out.writeBoolean(false);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        modificationTime = in.readLong();

        if (in.readBoolean()) {
            updateSpace = true;
            space = in.readLong();
            affRange = (IgfsFileAffinityRange)in.readObject();
        }
        else
            updateSpace = false;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter out = writer.rawWriter();

        out.writeLong(modificationTime);

        if (updateSpace) {
            out.writeBoolean(true);
            out.writeLong(space);
            out.writeObject(affRange);
        }
        else
            out.writeBoolean(false);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader in = reader.rawReader();

        modificationTime = in.readLong();

        if (in.readBoolean()) {
            updateSpace = true;
            space = in.readLong();
            affRange = in.readObject();
        }
        else
            updateSpace = false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsMetaFileUnlockProcessor.class, this);
    }
}
