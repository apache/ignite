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

import java.io.Serializable;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcRawBinarylizable;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Affinity function descriptor. Used to compare affinity functions of two tables.
 */
public class PartitionTableAffinityDescriptor implements Serializable, JdbcRawBinarylizable {
    /** */
    private static final long serialVersionUID = 1L;

    /** Affinity function type. */
    private final PartitionAffinityFunctionType affFunc;

    /** Number of partitions. */
    private final int parts;

    /** Whether node filter is set. */
    private final boolean hasNodeFilter;

    /** Data region name. */
    private final String dataRegion;

    /**
     * Constructor.
     *
     * @param affFunc Affinity function type.
     * @param parts Number of partitions.
     * @param hasNodeFilter Whether node filter is set.
     * @param dataRegion Data region.
     */
    public PartitionTableAffinityDescriptor(
        PartitionAffinityFunctionType affFunc,
        int parts,
        boolean hasNodeFilter,
        String dataRegion
    ) {
        this.affFunc = affFunc;
        this.parts = parts;
        this.hasNodeFilter = hasNodeFilter;
        this.dataRegion = dataRegion;
    }

    /**
     * Check is provided descriptor is compatible with this instance (i.e. can be used in the same co-location group).
     *
     * @param other Other descriptor.
     * @return {@code True} if compatible.
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean isCompatible(PartitionTableAffinityDescriptor other) {
        if (other == null)
            return false;

        // Rendezvous affinity function is deterministic and doesn't depend on previous cluster view changes.
        // In future other user affinity functions would be applicable as well if explicityl marked deterministic.
        if (affFunc == PartitionAffinityFunctionType.RENDEZVOUS) {
            // We cannot be sure that two caches are co-located if custom node filter is present.
            // Nota that technically we may try to compare two filters. However, this adds unnecessary complexity
            // and potential deserialization issues when SQL is called from client nodes or thin clients.
            if (!hasNodeFilter) {
                return
                    other.affFunc == PartitionAffinityFunctionType.RENDEZVOUS &&
                        !other.hasNodeFilter &&
                        other.parts == parts &&
                        F.eq(other.dataRegion, dataRegion);
            }
        }

        return false;
    }

    public boolean isClientBestEffortAffinityApplicable() {
        return affFunc == PartitionAffinityFunctionType.RENDEZVOUS && !hasNodeFilter;
    }

    /**
     * @return Number of partitions.
     */
    public int parts() {
        return parts;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionTableAffinityDescriptor.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer, ClientListenerProtocolVersion ver)
        throws BinaryObjectException {
        assert isClientBestEffortAffinityApplicable();

        writer.writeInt(parts);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader, ClientListenerProtocolVersion ver)
        throws BinaryObjectException {
        // No-op.
    }

    /**
     * Returns debinarized partition table affinity descriptor.
     *
     * @param reader Binary reader.
     * @param ver Protocol verssion.
     * @return Debinarized partition table affinity descriptor.
     * @throws BinaryObjectException On error.
     */
    public static PartitionTableAffinityDescriptor readTableAffinityDescriptor(BinaryReaderExImpl reader,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {

        return new PartitionTableAffinityDescriptor(PartitionAffinityFunctionType.RENDEZVOUS, reader.readInt(),
            false, null);
    }
}
