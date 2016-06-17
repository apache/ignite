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

package org.apache.ignite.internal.processors.igfs.client.meta;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.processors.igfs.IgfsContext;
import org.apache.ignite.internal.processors.igfs.IgfsFileAffinityRange;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientAbstractCallable;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Callable to unlock file info.
 */
public class IgfsClientMetaFileUnlockCallable extends IgfsClientAbstractCallable<Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** File ID. */
    private IgniteUuid fileId;

    /** Lock ID. */
    private IgniteUuid lockId;

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
    public IgfsClientMetaFileUnlockCallable() {
        // NO-op.
    }

    /**
     * Constructor.
     *
     * @param igfsName IGFS name.
     * @param fileId File ID.
     * @param lockId Lock ID.
     * @param modificationTime Modification time.
     */
    public IgfsClientMetaFileUnlockCallable(@Nullable String igfsName, IgniteUuid fileId, IgniteUuid lockId,
        long modificationTime, boolean updateSpace, long space, @Nullable IgfsFileAffinityRange affRange) {
        super(igfsName, null);

        this.fileId = fileId;
        this.lockId = lockId;
        this.modificationTime = modificationTime;
        this.updateSpace = updateSpace;
        this.space = space;
        this.affRange = affRange;
    }

    /** {@inheritDoc} */
    @Override protected Void call0(IgfsContext ctx) throws Exception {
        ctx.meta().unlock(fileId, lockId, modificationTime, updateSpace, space, affRange);

        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteUuid affinityKey() {
        return fileId;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary0(BinaryRawWriter writer) throws BinaryObjectException {
        BinaryUtils.writeIgniteUuid(writer, fileId);
        BinaryUtils.writeIgniteUuid(writer, lockId);

        writer.writeLong(modificationTime);

        if (updateSpace) {
            writer.writeBoolean(true);
            writer.writeLong(space);
            writer.writeObject(affRange);
        }
        else
            writer.writeBoolean(false);
    }

    /** {@inheritDoc} */
    @Override public void readBinary0(BinaryRawReader reader) throws BinaryObjectException {
        fileId = BinaryUtils.readIgniteUuid(reader);
        lockId = BinaryUtils.readIgniteUuid(reader);

        modificationTime = reader.readLong();

        if (reader.readBoolean()) {
            updateSpace = true;

            space = reader.readLong();
            affRange = reader.readObject();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsClientMetaFileUnlockCallable.class, this);
    }
}
