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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.internal.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;

/**
 * Internal key that is guaranteed to be mapped on particular partition.
 * This class is used for group-locking transactions that lock the whole partition.
 */
public class GridPartitionLockKey implements GridCacheInternal, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Partition ID. */
    private int partId;

    /** Key itself. */
    @GridToStringInclude
    private Object key;

    /**
     * Required by {@link Externalizable}.
     */
    public GridPartitionLockKey() {
        // No-op.
    }

    /**
     * @param key Key.
     */
    public GridPartitionLockKey(Object key) {
        assert key != null;

        this.key = key;
    }

    /**
     * @param partId Partition ID.
     */
    public void partitionId(int partId) {
        this.partId = partId;
    }

    /**
     * @return Partition ID.
     */
    public int partitionId() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (! (o instanceof GridPartitionLockKey))
            return false;

        GridPartitionLockKey that = (GridPartitionLockKey)o;

        return key.equals(that.key);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return key.hashCode();
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(GridPartitionLockKey.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(partId);
        out.writeObject(key);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        partId = in.readInt();
        key = in.readObject();
    }
}
