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

package org.apache.ignite.internal.processors.metastorage.persistence;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.List;
import java.util.function.LongFunction;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/** Version class for distributed metastorage. */
final class DistributedMetaStorageVersion extends IgniteDataTransferObject {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /** Version with id "0". */
    public static final DistributedMetaStorageVersion INITIAL_VERSION = new DistributedMetaStorageVersion(0L, 1L);

    /** Incremental rehashing considering new update information. */
    private static long nextHash(long hash, DistributedMetaStorageHistoryItem update) {
        return hash * 31L + update.longHash();
    }

    /**
     * Id is basically a total number of distributed metastorage updates in current cluster.
     * Increases incrementally on every update starting with zero.
     *
     * @see #INITIAL_VERSION
     */
    @GridToStringInclude
    private long id;

    /**
     * Hash of the whole updates list. Hashing algorinthm is almost the same as in {@link List#hashCode()}, but with
     * {@code long} value instead of {@code int}.
     */
    @GridToStringInclude
    private long hash;

    /** Default constructor for deserialization. */
    public DistributedMetaStorageVersion() {
        // No-op.
    }

    /**
     * Constructor with all fields.
     *
     * @param id Id.
     * @param hash Hash.
     */
    private DistributedMetaStorageVersion(long id, long hash) {
        this.id = id;
        this.hash = hash;
    }

    /**
     * Calculate next version considering passed update information.
     *
     * @param update Single update.
     * @return Next version.
     */
    public DistributedMetaStorageVersion nextVersion(DistributedMetaStorageHistoryItem update) {
        return new DistributedMetaStorageVersion(id + 1, nextHash(hash, update));
    }

    /**
     * Calculate next version considering passed update information.
     *
     * @param updates Updates collection.
     * @return Next version.
     */
    public DistributedMetaStorageVersion nextVersion(Collection<DistributedMetaStorageHistoryItem> updates) {
        long hash = this.hash;

        for (DistributedMetaStorageHistoryItem update : updates)
            hash = nextHash(hash, update);

        return new DistributedMetaStorageVersion(id + updates.size(), hash);
    }

    /**
     * Calculate next version considering passed update information.
     *
     * @param updates Updates array.
     * @param fromIdx Index of the first required update in the array.
     * @param toIdx Index after the last required update in the array.
     * @return Next version.
     */
    public DistributedMetaStorageVersion nextVersion(
        DistributedMetaStorageHistoryItem[] updates,
        int fromIdx,
        int toIdx // exclusive
    ) {
        long hash = this.hash;

        for (int idx = fromIdx; idx < toIdx; idx++)
            hash = nextHash(hash, updates[idx]);

        return new DistributedMetaStorageVersion(id + toIdx - fromIdx, hash);
    }

    /**
     * Calculate next version considering passed update information.
     *
     * @param update Function that provides the update by specific version.
     * @param fromVer Starting version, inclusive.
     * @param toVer Ending version, inclusive.
     * @return Next version.
     */
    public DistributedMetaStorageVersion nextVersion(
        LongFunction<DistributedMetaStorageHistoryItem> update,
        long fromVer,
        long toVer // inclusive
    ) {
        assert fromVer <= toVer;

        long hash = this.hash;

        for (long idx = fromVer; idx <= toVer; idx++)
            hash = nextHash(hash, update.apply(idx));

        return new DistributedMetaStorageVersion(id + toVer + 1 - fromVer, hash);
    }

    /**
     * Id is basically a total number of distributed metastorage updates in current cluster.
     * Increases incrementally on every update starting with zero.
     *
     * @see #INITIAL_VERSION
     */
    public long id() {
        return id;
    }

    /**
     * Hash of the whole updates list. Hashing algorinthm is almost the same as in {@link List#hashCode()}, but with
     * {@code long} value instead of {@code int}.
     */
    public long hash() {
        return hash;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeLong(id);
        out.writeLong(hash);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException {
        id = in.readLong();
        hash = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        DistributedMetaStorageVersion ver = (DistributedMetaStorageVersion)o;

        return id == ver.id && hash == ver.hash;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * Long.hashCode(id) + Long.hashCode(hash);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DistributedMetaStorageVersion.class, this);
    }
}
