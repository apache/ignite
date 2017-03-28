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

package org.apache.ignite.internal.processors.platform.cache.affinity;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Native cache wrapper implementation.
 */
@SuppressWarnings({"unchecked", "UnusedDeclaration", "TryFinallyCanBeTryWithResources"})
public class PlatformAffinity extends PlatformAbstractTarget {
    /** */
    public static final int OP_AFFINITY_KEY = 1;

    /** */
    public static final int OP_ALL_PARTITIONS = 2;

    /** */
    public static final int OP_BACKUP_PARTITIONS = 3;

    /** */
    public static final int OP_IS_BACKUP = 4;

    /** */
    public static final int OP_IS_PRIMARY = 5;

    /** */
    public static final int OP_IS_PRIMARY_OR_BACKUP = 6;

    /** */
    public static final int OP_MAP_KEY_TO_NODE = 7;

    /** */
    public static final int OP_MAP_KEY_TO_PRIMARY_AND_BACKUPS = 8;

    /** */
    public static final int OP_MAP_KEYS_TO_NODES = 9;

    /** */
    public static final int OP_MAP_PARTITION_TO_NODE = 10;

    /** */
    public static final int OP_MAP_PARTITION_TO_PRIMARY_AND_BACKUPS = 11;

    /** */
    public static final int OP_MAP_PARTITIONS_TO_NODES = 12;

    /** */
    public static final int OP_PARTITION = 13;

    /** */
    public static final int OP_PRIMARY_PARTITIONS = 14;

    /** */
    public static final int OP_PARTITIONS = 15;

    /** */
    private static final C1<ClusterNode, UUID> TO_NODE_ID = new C1<ClusterNode, UUID>() {
        @Nullable @Override public UUID apply(ClusterNode node) {
            return node != null ? node.id() : null;
        }
    };

    /** Underlying cache affinity. */
    private final Affinity<Object> aff;

    /** Discovery manager */
    private final GridDiscoveryManager discovery;

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     * @param igniteCtx Ignite context.
     * @param name Cache name.
     */
    public PlatformAffinity(PlatformContext platformCtx, GridKernalContext igniteCtx, @Nullable String name)
        throws IgniteCheckedException {
        super(platformCtx);

        this.aff = igniteCtx.grid().affinity(name);

        if (aff == null)
            throw new IgniteCheckedException("Cache with the given name doesn't exist: " + name);

        discovery = igniteCtx.discovery();
    }

    /** {@inheritDoc} */
    @Override public long processInStreamOutLong(int type, BinaryRawReaderEx reader) throws IgniteCheckedException {
        switch (type) {
            case OP_PARTITION:
                return aff.partition(reader.readObjectDetached());

            case OP_IS_PRIMARY: {
                UUID nodeId = reader.readUuid();

                Object key = reader.readObjectDetached();

                ClusterNode node = discovery.node(nodeId);

                if (node == null)
                    return FALSE;

                return aff.isPrimary(node, key) ? TRUE : FALSE;
            }

            case OP_IS_BACKUP: {
                UUID nodeId = reader.readUuid();

                Object key = reader.readObjectDetached();

                ClusterNode node = discovery.node(nodeId);

                if (node == null)
                    return FALSE;

                return aff.isBackup(node, key) ? TRUE : FALSE;
            }

            case OP_IS_PRIMARY_OR_BACKUP: {
                UUID nodeId = reader.readUuid();

                Object key = reader.readObjectDetached();

                ClusterNode node = discovery.node(nodeId);

                if (node == null)
                    return FALSE;

                return aff.isPrimaryOrBackup(node, key) ? TRUE : FALSE;
            }

            default:
                return super.processInStreamOutLong(type, reader);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"IfMayBeConditional", "ConstantConditions"})
    @Override public void processInStreamOutStream(int type, BinaryRawReaderEx reader, BinaryRawWriterEx writer)
        throws IgniteCheckedException {
        switch (type) {
            case OP_PRIMARY_PARTITIONS: {
                UUID nodeId = reader.readObject();

                ClusterNode node = discovery.node(nodeId);

                int[] parts = node != null ? aff.primaryPartitions(node) : U.EMPTY_INTS;

                writer.writeIntArray(parts);

                break;
            }

            case OP_BACKUP_PARTITIONS: {
                UUID nodeId = reader.readObject();

                ClusterNode node = discovery.node(nodeId);

                int[] parts = node != null ? aff.backupPartitions(node) : U.EMPTY_INTS;

                writer.writeIntArray(parts);

                break;
            }

            case OP_ALL_PARTITIONS: {
                UUID nodeId = reader.readObject();

                ClusterNode node = discovery.node(nodeId);

                int[] parts = node != null ? aff.allPartitions(node) : U.EMPTY_INTS;

                writer.writeIntArray(parts);

                break;
            }

            case OP_AFFINITY_KEY: {
                Object key = reader.readObjectDetached();

                writer.writeObject(aff.affinityKey(key));

                break;
            }

            case OP_MAP_KEY_TO_NODE: {
                Object key = reader.readObjectDetached();

                ClusterNode node = aff.mapKeyToNode(key);

                platformCtx.writeNode(writer, node);

                break;
            }

            case OP_MAP_PARTITION_TO_NODE: {
                int part = reader.readObject();

                ClusterNode node = aff.mapPartitionToNode(part);

                platformCtx.writeNode(writer, node);

                break;
            }

            case OP_MAP_KEY_TO_PRIMARY_AND_BACKUPS: {
                Object key = reader.readObjectDetached();

                platformCtx.writeNodes(writer, aff.mapKeyToPrimaryAndBackups(key));

                break;
            }

            case OP_MAP_PARTITION_TO_PRIMARY_AND_BACKUPS: {
                int part = reader.readObject();

                platformCtx.writeNodes(writer, aff.mapPartitionToPrimaryAndBackups(part));

                break;
            }

            case OP_MAP_KEYS_TO_NODES: {
                Collection<Object> keys = PlatformUtils.readCollection(reader);

                Map<ClusterNode, Collection<Object>> map = aff.mapKeysToNodes(keys);

                writer.writeInt(map.size());

                for (Map.Entry<ClusterNode, Collection<Object>> e : map.entrySet()) {
                    platformCtx.addNode(e.getKey());

                    writer.writeUuid(e.getKey().id());
                    writer.writeObject(e.getValue());
                }

                break;
            }

            case OP_MAP_PARTITIONS_TO_NODES: {
                Collection<Integer> parts = PlatformUtils.readCollection(reader);

                Map<Integer, ClusterNode> map = aff.mapPartitionsToNodes(parts);

                writer.writeInt(map.size());

                for (Map.Entry<Integer, ClusterNode> e : map.entrySet()) {
                    platformCtx.addNode(e.getValue());

                    writer.writeInt(e.getKey());

                    writer.writeUuid(e.getValue().id());
                }

                break;
            }

            default:
                super.processInStreamOutStream(type, reader, writer);
        }
    }

    /** {@inheritDoc} */
    @Override public long processInLongOutLong(int type, long val) throws IgniteCheckedException {
        if (type == OP_PARTITIONS)
            return aff.partitions();

        return super.processInLongOutLong(type, val);
    }
}
