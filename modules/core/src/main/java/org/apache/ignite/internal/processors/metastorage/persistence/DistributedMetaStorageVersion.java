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

import java.io.Serializable;
import java.util.Arrays;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/** */
class DistributedMetaStorageVersion implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final DistributedMetaStorageVersion INITIAL_VERSION = new DistributedMetaStorageVersion(0, 0);

    /** */
    public static long nextHash(long hash, DistributedMetaStorageHistoryItem update) {
        return hash * 31 + ((long)update.key.hashCode() << 32) + Arrays.hashCode(update.valBytes);
    }

    /** */
    @GridToStringInclude
    public final long id;

    /** */
    @GridToStringInclude
    public final long hash;

    /** */
    private DistributedMetaStorageVersion(long id, long hash) {
        this.id = id;
        this.hash = hash;
    }

    /** */
    public DistributedMetaStorageVersion nextVersion(DistributedMetaStorageHistoryItem update) {
        return new DistributedMetaStorageVersion(id + 1, nextHash(hash, update));
    }

    /** */
    public DistributedMetaStorageVersion nextVersion(Iterable<DistributedMetaStorageHistoryItem> updates) {
        long id = this.id;
        long hash = this.hash;

        for (DistributedMetaStorageHistoryItem update : updates) {
            ++id;

            hash = nextHash(hash, update);
        }

        return new DistributedMetaStorageVersion(id, hash);
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
