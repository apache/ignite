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

package org.apache.ignite.internal.processors.query.h2.affinity.tree;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Partition resolver.
 */
public class PartitionTableDescriptor {
    /** Indexing. */
    private final IgniteH2Indexing idx;

    /** Cache name. */
    private final String cacheName;

    /** Table name. */
    private final String tblName;

    /**
     * Constructor.
     *
     * @param idx Indexing.
     * @param cacheName Cache name.
     * @param tblName Table name.
     */
    public PartitionTableDescriptor(IgniteH2Indexing idx, String cacheName, String tblName) {
        this.idx = idx;
        this.cacheName = cacheName;
        this.tblName = tblName;
    }

    /**
     * Resolve partition.
     *
     * @param val Value.
     * @param dataType Data type.
     * @return Partition.
     */
    public int resolve(Object val, int dataType) {
        try {
            Object param = H2Utils.convert(val, idx, dataType);

            return idx.kernalContext().affinity().partition(cacheName, param);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to resolve partition: " + cacheName, e);
        }
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * cacheName.hashCode() + tblName.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o == this)
            return true;

        if (o.getClass() != getClass())
            return false;

        PartitionTableDescriptor other = (PartitionTableDescriptor)o;

        return F.eq(cacheName, other.cacheName) && F.eq(tblName, other.tblName);
    }
}
