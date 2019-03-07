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

package org.apache.ignite.internal.sql.optimizer.affinity;

import java.util.Collection;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcRawBinarylizable;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Partition extraction result.
 */
public class PartitionResult implements JdbcRawBinarylizable {
    /** Tree. */
    @GridToStringInclude
    private final PartitionNode tree;

    /** Affinity function. */
    private final PartitionTableAffinityDescriptor aff;

    /**
     * Constructor.
     *
     * @param tree Tree.
     * @param aff Affinity function.
     */
    public PartitionResult(PartitionNode tree, PartitionTableAffinityDescriptor aff) {
        this.tree = tree;
        this.aff = aff;
    }

    /**
     * Tree.
     */
    public PartitionNode tree() {
        return tree;
    }

    /**
     * @return Affinity function.
     */
    public PartitionTableAffinityDescriptor affinity() {
        return aff;
    }

    /**
     * Calculate partitions for the query.
     *
     * @param explicitParts Explicit partitions provided in SqlFieldsQuery.partitions property.
     * @param derivedParts Derived partitions found during partition pruning.
     * @param args Arguments.
     * @return Calculated partitions or {@code null} if failed to calculate and there should be a broadcast.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    public static int[] calculatePartitions(int[] explicitParts, PartitionResult derivedParts, Object[] args) {
        if (!F.isEmpty(explicitParts))
            return explicitParts;
        else if (derivedParts != null) {
            try {
                Collection<Integer> realParts = derivedParts.tree().apply(null, args);

                if (realParts == null)
                    return null;
                else if (realParts.isEmpty())
                    return IgniteUtils.EMPTY_INTS;
                else {
                    int[] realParts0 = new int[realParts.size()];

                    int i = 0;

                    for (Integer realPart : realParts)
                        realParts0[i++] = realPart;

                    return realParts0;
                }
            }
            catch (IgniteCheckedException e) {
                throw new CacheException("Failed to calculate derived partitions for query.", e);
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionResult.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer, ClientListenerProtocolVersion ver)
        throws BinaryObjectException {
        writer.writeBoolean(tree != null);

        if (tree != null)
            tree.writeBinary(writer, ver);

        // TODO: 26.02.19 write PartitionTableAffinityDescriptor?
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader, ClientListenerProtocolVersion ver)
        throws BinaryObjectException {
        // No-op
    }
}
