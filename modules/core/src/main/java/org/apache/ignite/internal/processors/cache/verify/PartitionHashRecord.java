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
package org.apache.ignite.internal.processors.cache.verify;

import java.io.Serializable;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Record containing partition checksum, primary flag and consistent ID of owner.
 */
public class PartitionHashRecord implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Partition key. */
    @GridToStringExclude
    private final PartitionKey partKey;

    /** Is primary flag. */
    private final boolean isPrimary;

    /** Consistent id. */
    private final Object consistentId;

    /** Partition hash. */
    private final int partHash;

    /** Update counter. */
    private final long updateCntr;

    /** Size. */
    private final long size;

    /**
     * @param partKey Partition key.
     * @param isPrimary Is primary.
     * @param consistentId Consistent id.
     * @param partHash Partition hash.
     * @param updateCntr Update counter.
     * @param size Size.
     */
    public PartitionHashRecord(PartitionKey partKey, boolean isPrimary,
        Object consistentId, int partHash, long updateCntr, long size) {
        this.partKey = partKey;
        this.isPrimary = isPrimary;
        this.consistentId = consistentId;
        this.partHash = partHash;
        this.updateCntr = updateCntr;
        this.size = size;
    }

    /**
     * @return Partition key.
     */
    public PartitionKey partitionKey() {
        return partKey;
    }

    /**
     * @return Is primary.
     */
    public boolean isPrimary() {
        return isPrimary;
    }

    /**
     * @return Consistent id.
     */
    public Object consistentId() {
        return consistentId;
    }

    /**
     * @return Partition hash.
     */
    public int partitionHash() {
        return partHash;
    }

    /**
     * @return Update counter.
     */
    public long updateCounter() {
        return updateCntr;
    }

    /**
     * @return Size.
     */
    public long size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionHashRecord.class, this, "consistentId", consistentId);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PartitionHashRecord record = (PartitionHashRecord)o;

        return consistentId.equals(record.consistentId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return consistentId.hashCode();
    }
}
