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
