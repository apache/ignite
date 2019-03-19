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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Node with partition which should be extracted from argument.
 */
public class PartitionParameterNode extends PartitionSingleNode {
    /** Indexing. */
    @GridToStringExclude
    private final PartitionResolver partRslvr;

    /** Index. */
    private final int idx;

    /** Parameter data type. */
    private final int type;

    /** Mapped parameter type. */
    private final PartitionParameterType mappedType;

    /**
     * Constructor.
     *
     * @param tbl Table descriptor.
     * @param partRslvr Partition resolver.
     * @param idx Parameter index.
     * @param type Parameter data type.
     * @param mappedType Mapped parameter type to be used by thin clients.
     */
    public PartitionParameterNode(PartitionTable tbl, PartitionResolver partRslvr, int idx, int type,
        PartitionParameterType mappedType) {
        super(tbl);

        this.partRslvr = partRslvr;
        this.idx = idx;
        this.type = type;
        this.mappedType = mappedType;
    }

    /** {@inheritDoc} */
    @Override public Integer applySingle(PartitionClientContext cliCtx, Object... args) throws IgniteCheckedException {
        assert args != null;
        assert idx < args.length;

        Object arg = args[idx];

        if (cliCtx != null)
            return cliCtx.partition(arg, mappedType, tbl.cacheName());
        else {
            assert partRslvr != null;

            return partRslvr.partition(
                arg,
                type,
                tbl.cacheName()
            );
        }
    }

    /** {@inheritDoc} */
    @Override public boolean constant() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int value() {
        return idx;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionParameterNode.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer, ClientListenerProtocolVersion ver)
        throws BinaryObjectException {
        writer.writeByte(PARAM_NODE);

        writer.writeInt(idx);

        writer.writeInt(type);

        writer.writeInt(mappedType.ordinal());

        tbl.writeBinary(writer, ver);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader, ClientListenerProtocolVersion ver)
        throws BinaryObjectException {
        // No-op.
    }

    /**
     * Returns debinarized parameter node.
     *
     * @param reader Binary reader.
     * @param ver Protocol verssion.
     * @return Debinarized parameter node.
     * @throws BinaryObjectException On error.
     */
    public static PartitionParameterNode readParameterNode(BinaryReaderExImpl reader, ClientListenerProtocolVersion ver)
        throws BinaryObjectException {

        int idx = reader.readInt();

        int type = reader.readInt();

        PartitionParameterType mappedType = PartitionParameterType.readParameterType(reader, ver);

        PartitionTable tbl = PartitionTable.readTable(reader, ver);

        return new PartitionParameterNode(tbl, null, idx, type, mappedType);
    }
}
