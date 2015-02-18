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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Base for all messages in replicated cache.
 */
public abstract class GridDistributedBaseMessage<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable,
    GridCacheVersionable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Lock or transaction version. */
    @GridToStringInclude
    protected GridCacheVersion ver;

    /**
     * Candidates for every key ordered in the order of keys. These
     * can be either local-only candidates in case of lock acquisition,
     * or pending candidates in case of transaction commit.
     */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<GridCacheMvccCandidate<K>>[] candsByIdx;

    /** */
    @GridToStringExclude
    private byte[] candsByIdxBytes;

    /** Collections of local lock candidates. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<K, Collection<GridCacheMvccCandidate<K>>> candsByKey;

    /** Collections of local lock candidates in serialized form. */
    @GridToStringExclude
    private byte[] candsByKeyBytes;

    /** Committed versions with order higher than one for this message (needed for commit ordering). */
    @GridToStringInclude
    @GridDirectCollection(GridCacheVersion.class)
    private Collection<GridCacheVersion> committedVers;

    /** Rolled back versions with order higher than one for this message (needed for commit ordering). */
    @GridToStringInclude
    @GridDirectCollection(GridCacheVersion.class)
    private Collection<GridCacheVersion> rolledbackVers;

    /** Count of keys referenced in candidates array (needed only locally for optimization). */
    @GridToStringInclude
    @GridDirectTransient
    private int cnt;

    /**
     * Empty constructor required by {@link Externalizable}
     */
    protected GridDistributedBaseMessage() {
        /* No-op. */
    }

    /**
     * @param cnt Count of keys references in list of candidates.
     */
    protected GridDistributedBaseMessage(int cnt) {
        assert cnt >= 0;

        this.cnt = cnt;
    }

    /**
     * @param ver Either lock or transaction version.
     * @param cnt Key count.
     */
    protected GridDistributedBaseMessage(GridCacheVersion ver, int cnt) {
        this(cnt);

        assert ver != null;

        this.ver = ver;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (candsByIdx != null)
            candsByIdxBytes = ctx.marshaller().marshal(candsByIdx);

        if (candsByKey != null) {
            if (ctx.deploymentEnabled()) {
                for (K key : candsByKey.keySet())
                    prepareObject(key, ctx);
            }

            candsByKeyBytes = CU.marshal(ctx, candsByKey);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (candsByIdxBytes != null)
            candsByIdx = ctx.marshaller().unmarshal(candsByIdxBytes, ldr);

        if (candsByKeyBytes != null)
            candsByKey = ctx.marshaller().unmarshal(candsByKeyBytes, ldr);
    }

    /**
     * @return Version.
     */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /**
     * @param ver Version.
     */
    public void version(GridCacheVersion ver) {
        this.ver = ver;
    }

    /**
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
     */
    public void completedVersions(Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers) {
        this.committedVers = committedVers;
        this.rolledbackVers = rolledbackVers;
    }

    /**
     * @return Committed versions.
     */
    public Collection<GridCacheVersion> committedVersions() {
        return committedVers == null ? Collections.<GridCacheVersion>emptyList() : committedVers;
    }

    /**
     * @return Rolled back versions.
     */
    public Collection<GridCacheVersion> rolledbackVersions() {
        return rolledbackVers == null ? Collections.<GridCacheVersion>emptyList() : rolledbackVers;
    }

    /**
     * @param idx Key index.
     * @param candsByIdx List of candidates for that key.
     */
    @SuppressWarnings({"unchecked"})
    public void candidatesByIndex(int idx, Collection<GridCacheMvccCandidate<K>> candsByIdx) {
        assert idx < cnt;

        // If nothing to add.
        if (candsByIdx == null || candsByIdx.isEmpty())
            return;

        if (this.candsByIdx == null)
            this.candsByIdx = new Collection[cnt];

        this.candsByIdx[idx] = candsByIdx;
    }

    /**
     * @param idx Key index.
     * @return Candidates for given key.
     */
    public Collection<GridCacheMvccCandidate<K>> candidatesByIndex(int idx) {
        return candsByIdx == null || candsByIdx[idx] == null ? Collections.<GridCacheMvccCandidate<K>>emptyList() : candsByIdx[idx];
    }

    /**
     * @param key Candidates key.
     * @param candsByKey Collection of local candidates.
     */
    public void candidatesByKey(K key, Collection<GridCacheMvccCandidate<K>> candsByKey) {
        if (this.candsByKey == null)
            this.candsByKey = new HashMap<>(1, 1.0f);

        this.candsByKey.put(key, candsByKey);
    }

    /**
     *
     * @param key Candidates key.
     * @return Collection of lock candidates at given index.
     */
    @Nullable public Collection<GridCacheMvccCandidate<K>> candidatesByKey(K key) {
        assert key != null;

        if (candsByKey == null)
            return null;

        return candsByKey.get(key);
    }

    /**
     * @return Map of candidates.
     */
    public Map<K, Collection<GridCacheMvccCandidate<K>>> candidatesByKey() {
        return candsByKey == null ? Collections.<K, Collection<GridCacheMvccCandidate<K>>>emptyMap() : candsByKey;
    }

    /**
     * @return Count of keys referenced in candidates array (needed only locally for optimization).
     */
    public int keysCount() {
        return cnt;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(new MessageHeader(directType(), (byte)8)))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 3:
                if (!writer.writeByteArray("candsByIdxBytes", candsByIdxBytes))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeByteArray("candsByKeyBytes", candsByKeyBytes))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeCollection("committedVers", committedVers, Type.MSG))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeCollection("rolledbackVers", rolledbackVers, Type.MSG))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeMessage("ver", ver))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (readState) {
            case 3:
                candsByIdxBytes = reader.readByteArray("candsByIdxBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 4:
                candsByKeyBytes = reader.readByteArray("candsByKeyBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 5:
                committedVers = reader.readCollection("committedVers", Type.MSG);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 6:
                rolledbackVers = reader.readCollection("rolledbackVers", Type.MSG);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 7:
                ver = reader.readMessage("ver");

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDistributedBaseMessage.class, this, "super", super.toString());
    }
}
