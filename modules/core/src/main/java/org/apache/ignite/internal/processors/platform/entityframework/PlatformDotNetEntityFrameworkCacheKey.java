/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.platform.entityframework;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * EntityFramework cache key: query + versions.
 */
@SuppressWarnings("WeakerAccess")
public class PlatformDotNetEntityFrameworkCacheKey
    implements Binarylizable, Comparable<PlatformDotNetEntityFrameworkCacheKey> {
    /** Query text. */
    private String query;

    /** Entity set versions. */
    private long[] versions;

    /**
     * Ctor.
     */
    public PlatformDotNetEntityFrameworkCacheKey() {
        // No-op.
    }

    /**
     * Ctor.
     *
     * @param query Query text.
     * @param versions Versions.
     */
    PlatformDotNetEntityFrameworkCacheKey(String query, long[] versions) {
        assert query != null;

        this.query = query;
        this.versions = versions;
    }

    /**
     * Gets the query text.
     *
     * @return Query text.
     */
    public String query() {
        return query;
    }

    /**
     * Gets the entity set versions.
     *
     * @return Entity set versions.
     */
    public long[] versions() {
        return versions;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        PlatformDotNetEntityFrameworkCacheKey key = (PlatformDotNetEntityFrameworkCacheKey)o;

        //noinspection SimplifiableIfStatement
        if (!query.equals(key.query))
            return false;

        return Arrays.equals(versions, key.versions);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = query.hashCode();

        result = 31 * result + Arrays.hashCode(versions);

        return result;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        final BinaryRawWriter raw = writer.rawWriter();

        raw.writeString(query);

        if (versions != null) {
            raw.writeInt(versions.length);

            for (long ver : versions)
                raw.writeLong(ver);
        }
        else
            raw.writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader raw = reader.rawReader();

        query = raw.readString();

        int cnt = raw.readInt();

        if (cnt >= 0) {
            versions = new long[cnt];

            for (int i = 0; i < cnt; i++)
                versions[i] = raw.readLong();
        }
        else
            versions = null;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull PlatformDotNetEntityFrameworkCacheKey o) {
        int cmpQuery = query.compareTo(o.query);

        if (cmpQuery != 0)
            return cmpQuery;

        if (versions == null) {
            return o.versions == null ? 0 : -1;
        }

        if (o.versions == null)
            return 1;

        assert versions.length == o.versions.length;

        for (int i = 0; i < versions.length; i++) {
            if (versions[i] != o.versions[i]) {
                return versions[i] > o.versions[i] ? 1 : -1;
            }
        }

        return 0;
    }
}
