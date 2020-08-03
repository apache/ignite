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

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;

/**
 * Marshaller that lets to serialize and deserialize partiton result for the purposes of jdbc thin client size best
 * effort affinity.
 */
public class PartitionResultMarshaler {
    /** {@link PartitionCompositeNode} type. */
    static final byte COMPOSITE_NODE = 1;

    /** {@link PartitionConstantNode} type. */
    static final byte CONST_NODE = 2;

    /** {@link PartitionGroupNode} type. */
    static final byte GROUP_NODE = 3;

    /** {@link PartitionParameterNode} type. */
    static final byte PARAM_NODE = 4;

    /**
     * Writes partition result to provided writer.
     *
     * @param writer Binary object writer.
     * @param partRes Partitoin result to serialize.
     * @throws BinaryObjectException In case of error.
     */
    public static void marshal(BinaryWriterExImpl writer, PartitionResult partRes)
        throws BinaryObjectException {

        writeNode(writer, partRes.tree());

        writer.writeString(partRes.cacheName());

        writer.writeInt(partRes.partitionsCount());

        writer.writeLong(partRes.topologyVersion().topologyVersion());

        writer.writeInt(partRes.topologyVersion().minorTopologyVersion());
    }

    /**
     * Reads fields from provided reader.
     *
     * @param reader Binary object reader.
     * @return Deserialized partition result.
     * @throws BinaryObjectException In case of error.
     */
    public static PartitionResult unmarshal(BinaryReaderExImpl reader) throws BinaryObjectException {
        PartitionNode tree = readNode(reader);

        String cacheName = reader.readString();

        int partsCnt = reader.readInt();

        AffinityTopologyVersion topVer = new AffinityTopologyVersion(reader.readLong(), reader.readInt());

        return new PartitionResult(tree, topVer, cacheName, partsCnt);
    }

    /**
     * Returns deserialized partition node.
     *
     * @param reader Binary reader.
     * @return Deserialized partition node.
     * @throws BinaryObjectException In case of error.
     */
    private static PartitionNode readNode(BinaryReaderExImpl reader) throws BinaryObjectException {
        int nodeType = reader.readByte();

        switch (nodeType) {
            case COMPOSITE_NODE:
                return readCompositeNode(reader);

            case CONST_NODE:
                return readConstantNode(reader);

            case GROUP_NODE:
                return readGroupNode(reader);

            case PARAM_NODE:
                return readParameterNode(reader);

            default:
                throw new IllegalArgumentException("Partition node type " + nodeType + " not supported.");
        }
    }

    /**
     * Writes partition node to provided writer.
     *
     * @param writer Binary object writer.
     * @param node Partition node to serialize.
     * @throws BinaryObjectException In case of error.
     */
    private static void writeNode(BinaryWriterExImpl writer, PartitionNode node)
        throws BinaryObjectException {
        assert !(node instanceof PartitionAllNode);
        assert !(node instanceof PartitionNoneNode);

        if (node instanceof PartitionCompositeNode)
            writeCompositeNode(writer, (PartitionCompositeNode)node);
        else if (node instanceof PartitionConstantNode)
            writeConstantNode(writer, (PartitionConstantNode)node);
        else if (node instanceof PartitionGroupNode)
            writeGroupNode(writer, (PartitionGroupNode)node);
        else if (node instanceof PartitionParameterNode)
            writeParameterNode(writer, (PartitionParameterNode)node);
        else
            throw new IllegalArgumentException("Partition node type " + node.getClass() + " not supported.");
    }

    /**
     * Returns debinarized partition node.
     *
     * @param reader Binary reader.
     * @return Debinarized partition node.
     * @throws BinaryObjectException On error.
     */
    @SuppressWarnings("unused")
    private static PartitionConstantNode readConstantNode(BinaryReaderExImpl reader)
        throws BinaryObjectException {
        int part = reader.readInt();

        return new PartitionConstantNode(null, part);
    }

    /**
     * Writes partition constant node to provided writer.
     *
     * @param writer Binary object writer.
     * @param node Partition constant node to serialize.
     * @throws BinaryObjectException In case of error.
     */
    @SuppressWarnings("unused")
    private static void writeConstantNode(BinaryWriterExImpl writer, PartitionConstantNode node)
        throws BinaryObjectException {
        writer.writeByte(CONST_NODE);

        writer.writeInt(node.value());
    }

    /**
     * Returns debinarized partition composite node.
     *
     * @param reader Binary reader.
     * @return Debinarized partition composite node.
     * @throws BinaryObjectException On error.
     */
    private static PartitionCompositeNode readCompositeNode(BinaryReaderExImpl reader) throws BinaryObjectException {
        PartitionCompositeNodeOperator op = PartitionCompositeNodeOperator.fromOrdinal(reader.readInt());

        PartitionNode left = readNode(reader);

        PartitionNode right = readNode(reader);

        return new PartitionCompositeNode(left, right, op);
    }

    /**
     * Writes partition composite node to provided writer.
     *
     * @param writer Binary object writer.
     * @param node Partition composite node to serialize.
     * @throws BinaryObjectException In case of error.
     */
    private static void writeCompositeNode(BinaryWriterExImpl writer, PartitionCompositeNode node)
        throws BinaryObjectException {
        writer.writeByte(COMPOSITE_NODE);

        writer.writeInt(node.operator().ordinal());

        writeNode(writer, node.left());

        writeNode(writer, node.right());
    }

    /**
     * Returns debinarized partition group node.
     *
     * @param reader Binary reader.
     * @return Debinarized partition group node.
     * @throws BinaryObjectException On error.
     */
    private static PartitionGroupNode readGroupNode(BinaryReaderExImpl reader)
        throws BinaryObjectException {
        int siblingsCnt = reader.readInt();

        Set<PartitionSingleNode> siblings = new HashSet<>(siblingsCnt);

        for (int i = 0; i < siblingsCnt; i++) {
            int nodeType = reader.readByte();

            switch (nodeType) {
                case CONST_NODE:
                    siblings.add(readConstantNode(reader));
                    break;

                case PARAM_NODE:
                    siblings.add(readParameterNode(reader));
                    break;

                default:
                    throw new IllegalArgumentException("Partition node type " + nodeType + " is not valid signle node.");
            }
        }

        return new PartitionGroupNode(siblings);
    }

    /**
     * Writes partition group node to provided writer.
     *
     * @param writer Binary object writer.
     * @param node Partition group node to serialize.
     * @throws BinaryObjectException In case of error.
     */
    private static void writeGroupNode(BinaryWriterExImpl writer, PartitionGroupNode node)
        throws BinaryObjectException {
        writer.writeByte(GROUP_NODE);

        Set<PartitionSingleNode> siblings = node.siblings();

        assert siblings != null;

        writer.writeInt(siblings.size());

        for (PartitionSingleNode singleNode : siblings) {
            if (singleNode instanceof PartitionConstantNode)
                writeConstantNode(writer, (PartitionConstantNode)singleNode);
            else if (singleNode instanceof PartitionParameterNode)
                writeParameterNode(writer, (PartitionParameterNode)singleNode);
            else
                throw new IllegalArgumentException("Partition node type " + singleNode.getClass() + " not supported.");
        }
    }

    /**
     * Returns debinarized parameter node.
     *
     * @param reader Binary reader.
     * @return Debinarized parameter node.
     * @throws BinaryObjectException On error.
     */
    private static PartitionParameterNode readParameterNode(BinaryReaderExImpl reader) throws BinaryObjectException {

        int idx = reader.readInt();

        PartitionParameterType clientType = PartitionParameterType.fromOrdinal(reader.readInt());

        return new PartitionParameterNode(null, null, idx, -1, clientType);
    }

    /**
     * Writes partition parameter node to provided writer.
     *
     * @param writer Binary object writer.
     * @param node Partition parameter node to serialize.
     * @throws BinaryObjectException In case of error.
     */
    @SuppressWarnings("unused")
    private static void writeParameterNode(BinaryWriterExImpl writer, PartitionParameterNode node)
        throws BinaryObjectException {
        writer.writeByte(PARAM_NODE);

        writer.writeInt(node.value());

        writer.writeInt(node.clientType().ordinal());
    }
}
