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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcRawBinarylizable;
import org.jetbrains.annotations.Nullable;

/**
 * Common node of partition tree.
 */
public interface PartitionNode extends JdbcRawBinarylizable {

    /** {@link PartitionAllNode} type. */
    static final byte ALL_NODE = 1;

    /** {@link PartitionCompositeNode} type. */
    static final byte COMPOSITE_NODE = 2;

    /** {@link PartitionConstantNode} type. */
    static final byte CONST_NODE = 3;

    /** {@link PartitionGroupNode} type. */
    static final byte GROUP_NODE = 4;

    /** {@link PartitionNoneNode} type. */
    static final byte NONE_NODE = 5;

    /** {@link PartitionParameterNode} type. */
    static final byte PARAM_NODE = 6;

    /**
     * Get partitions.
     *
     * @param cliCtx Thin client context (optional).
     * @param args Query arguments.
     * @return Partitions.
     * @throws IgniteCheckedException If failed.
     */
    Collection<Integer> apply(@Nullable PartitionClientContext cliCtx, Object... args) throws IgniteCheckedException;

    /**
     * @return Join group for the given node.
     */
    int joinGroup();

    /**
     * Try optimizing partition nodes into a simpler form.
     *
     * @return Optimized node or {@code this} if optimization failed.
     */
    default PartitionNode optimize() {
        return this;
    }

    /**
     * Returns debinarized partition node.
     *
     * @param reader Binary reader.
     * @param ver Protocol verssion.
     * @return Debinarized partition node.
     * @throws BinaryObjectException On error.
     */
    public static PartitionNode readNode(BinaryReaderExImpl reader, ClientListenerProtocolVersion ver)
        throws BinaryObjectException {
        int nodeType = reader.readByte();

        switch (nodeType) {
            case ALL_NODE:
                return PartitionAllNode.INSTANCE;

            case COMPOSITE_NODE:
                return PartitionCompositeNode.readCompositeNode(reader, ver);

            case CONST_NODE:
                return PartitionConstantNode.readConstantNode(reader, ver);

            case GROUP_NODE:
                return PartitionGroupNode.readGroupNode(reader, ver);

            case NONE_NODE:
                return PartitionNoneNode.INSTANCE;

            case PARAM_NODE:
                return PartitionParameterNode.readParameterNode(reader, ver);

            default:
                throw new IllegalArgumentException("Partition node type " + nodeType + " not supported.");
        }
    }
}
