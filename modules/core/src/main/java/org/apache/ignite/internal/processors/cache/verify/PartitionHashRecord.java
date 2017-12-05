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
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Record containing partition checksum, primary flag and consistent ID of owner.
 */
public class PartitionHashRecord implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Is primary flag. */
    private boolean isPrimary;

    /** Consistent id. */
    private Object consistentId;

    /** Partition hash. */
    private int partHash;

    /**
     * @param isPrimary Is primary.
     * @param consistentId Consistent id.
     * @param partHash Partition hash.
     */
    public PartitionHashRecord(boolean isPrimary, Object consistentId, int partHash) {
        this.isPrimary = isPrimary;
        this.consistentId = consistentId;
        this.partHash = partHash;
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionHashRecord.class, this);
    }
}
