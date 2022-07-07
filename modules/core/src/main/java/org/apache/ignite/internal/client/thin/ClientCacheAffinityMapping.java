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

package org.apache.ignite.internal.client.thin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.ToIntBiFunction;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.internal.binary.BinaryObjectExImpl;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Affinity mapping (partition to nodes) for each cache.
 */
public class ClientCacheAffinityMapping {
    /** CacheAffinityInfo for caches with not applicable partition awareness. */
    private static final CacheAffinityInfo NOT_APPLICABLE_CACHE_AFFINITY_INFO =
        new CacheAffinityInfo(null, null, null);

    /** Topology version. */
    private final AffinityTopologyVersion topVer;

    /** Affinity information for each cache. */
    private final Map<Integer, CacheAffinityInfo> cacheAffinity = new HashMap<>();

    /** Unmodifiable collection of cache IDs. To preserve instance immutability. */
    private final Collection<Integer> cacheIds = Collections.unmodifiableCollection(cacheAffinity.keySet());

    /**
     * @param topVer Topology version.
     */
    private ClientCacheAffinityMapping(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /**
     * Gets topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * Gets cache IDs.
     */
    public Collection<Integer> cacheIds() {
        return cacheIds;
    }

    /**
     * Calculates affinity node for given cache and key.
     *
     * @param binary Binary data processor (needed to extract affinity field from the key).
     * @param cacheId Cache ID.
     * @param key Key.
     * @return Affinity node id or {@code null} if affinity node can't be determined for given cache and key.
     */
    public UUID affinityNode(IgniteBinary binary, int cacheId, Object key) {
        CacheAffinityInfo affinityInfo = cacheAffinity.get(cacheId);

        if (affinityInfo == null || affinityInfo == NOT_APPLICABLE_CACHE_AFFINITY_INFO)
            return null;

        Object binaryKey = binary.toBinary(key);

        if (!affinityInfo.keyCfg.isEmpty()) {
            int typeId = binary.typeId(key.getClass().getName());

            Integer fieldId = affinityInfo.keyCfg.get(typeId);

            if (fieldId != null) {
                if (binaryKey instanceof BinaryObjectExImpl)
                    binaryKey = ((BinaryObjectExImpl)binaryKey).field(fieldId);
                else // Can't get field value, affinity node can't be determined in this case.
                    return null;
            }
        }

        return affinityInfo.nodeForKey(binaryKey);
    }

    /**
     * Calculates affinity node for given cache and partition.
     *
     * @param cacheId Cache ID.
     * @param part Partition.
     * @return Affinity node id or {@code null} if affinity node can't be determined for given cache and partition.
     */
    public UUID affinityNode(int cacheId, int part) {
        CacheAffinityInfo affInfo = cacheAffinity.get(cacheId);

        if (affInfo == null || affInfo == NOT_APPLICABLE_CACHE_AFFINITY_INFO)
            return null;

        return affInfo.nodeForPartition(part);
    }

    /**
     * Merge specified mappings into one instance.
     */
    public static ClientCacheAffinityMapping merge(ClientCacheAffinityMapping... mappings) {
        assert !F.isEmpty(mappings);

        ClientCacheAffinityMapping res = new ClientCacheAffinityMapping(mappings[0].topVer);

        for (ClientCacheAffinityMapping mapping : mappings) {
            assert res.topVer.equals(mapping.topVer) : "Mappings must have identical topology versions [res.topVer=" +
                res.topVer + ", mapping.topVer=" + mapping.topVer + ']';

            res.cacheAffinity.putAll(mapping.cacheAffinity);
        }

        return res;
    }

    /**
     * Writes caches affinity request to the output channel.
     *
     * @param ch Output channel.
     * @param cacheIds Cache IDs.
     */
    public static void writeRequest(PayloadOutputChannel ch, Collection<Integer> cacheIds) {
        BinaryOutputStream out = ch.out();

        out.writeInt(cacheIds.size());

        for (int cacheId : cacheIds)
            out.writeInt(cacheId);
    }

    /**
     * Reads caches affinity response from the input channel and creates {@code ClientCacheAffinityMapping} instance
     * from this response.
     *
     * @param ch Input channel.
     * @param mappers Cache key to partition mappers.
     */
    public static ClientCacheAffinityMapping readResponse(
        PayloadInputChannel ch,
        Function<Integer, ToIntBiFunction<Object, Integer>> mappers
    ) {
        try (BinaryReaderExImpl in = ClientUtils.createBinaryReader(null, ch.in())) {
            long topVer = in.readLong();
            int minorTopVer = in.readInt();

            ClientCacheAffinityMapping aff = new ClientCacheAffinityMapping(
                new AffinityTopologyVersion(topVer, minorTopVer));

            int mappingsCnt = in.readInt();

            for (int i = 0; i < mappingsCnt; i++) {
                boolean applicable = in.readBoolean();

                int cachesCnt = in.readInt();

                if (applicable) { // Partition awareness is applicable for these caches.
                    Map<Integer, Map<Integer, Integer>> cacheKeyCfg = U.newHashMap(cachesCnt);

                    for (int j = 0; j < cachesCnt; j++)
                        cacheKeyCfg.put(in.readInt(), readCacheKeyConfiguration(in));

                    UUID[] partToNode = readNodePartitions(in);

                    boolean dfltAffinity = true;

                    if (ch.clientChannel().protocolCtx().isFeatureSupported(ProtocolBitmaskFeature.ALL_AFFINITY_MAPPINGS))
                        dfltAffinity = in.readBoolean();

                    boolean finalDfltAffinity = dfltAffinity;

                    for (Map.Entry<Integer, Map<Integer, Integer>> keyCfg : cacheKeyCfg.entrySet()) {
                        addCacheAffinityMapping(aff, keyCfg.getKey(), keyCfg.getValue(), partToNode,
                            parts -> finalDfltAffinity ? new RendezvousAffinityKeyMapper(parts) : mappers.apply(keyCfg.getKey()));
                    }
                }
                else { // Partition awareness is not applicable for these caches.
                    for (int j = 0; j < cachesCnt; j++)
                        addCacheAffinityMapping(aff, in.readInt(), null, null, null);
                }
            }

            return aff;
        }
        catch (IOException e) {
            throw new ClientError(e);
        }
    }

    /**
     * @param in Input reader.
     */
    private static Map<Integer, Integer> readCacheKeyConfiguration(BinaryReaderExImpl in) {
        int keyCfgCnt = in.readInt();

        Map<Integer, Integer> keyCfg = U.newHashMap(keyCfgCnt);

        for (int i = 0; i < keyCfgCnt; i++)
            keyCfg.put(in.readInt(), in.readInt());

        return keyCfg;
    }

    /**
     * @param in Input reader.
     */
    private static UUID[] readNodePartitions(BinaryReaderExImpl in) {
        int nodesCnt = in.readInt();

        int maxPart = -1;

        UUID[] partToNode = new UUID[1024];

        for (int i = 0; i < nodesCnt; i++) {
            UUID nodeId = in.readUuid();

            int partCnt = in.readInt();

            for (int j = 0; j < partCnt; j++) {
                int part = in.readInt();

                if (part > maxPart) {
                    maxPart = part;

                    // Expand partToNode if needed.
                    if (part >= partToNode.length)
                        partToNode = Arrays.copyOf(partToNode, U.ceilPow2(part + 1));
                }

                partToNode[part] = nodeId;
            }
        }

        return Arrays.copyOf(partToNode, maxPart + 1);
    }

    /**
     * @param cacheId Cache id.
     * @param keyCfg Cache configuration mapping.
     * @param partMapping Partition mapping to node.
     * @param mapperFactory Cache key mapper factory.
     */
    private static void addCacheAffinityMapping(
        ClientCacheAffinityMapping mapping,
        Integer cacheId,
        Map<Integer, Integer> keyCfg,
        UUID[] partMapping,
        IntFunction<ToIntBiFunction<Object, Integer>> mapperFactory
    ) {
        mapping.cacheAffinity.put(cacheId,
            partMapping == null ? NOT_APPLICABLE_CACHE_AFFINITY_INFO :
                new CacheAffinityInfo(keyCfg, partMapping, mapperFactory.apply(partMapping.length)));
    }

    /**
     * Class to store affinity information for cache.
     */
    private static class CacheAffinityInfo {
        /** Key configuration. */
        private final Map<Integer, Integer> keyCfg;

        /** Partition mapping. */
        private final UUID[] partMapping;

        /** Total number of partitions. */
        private final Integer parts;

        /** Mapper a cache key to a partition. */
        private final ToIntBiFunction<Object, Integer> keyMapper;

        /**
         * @param keyCfg Cache key configuration or {@code null} if partition awareness is not applicable for this cache.
         * @param partMapping Partition to node mapping or {@code null} if partition awareness is not applicable for
         * this cache.
         */
        private CacheAffinityInfo(Map<Integer, Integer> keyCfg, UUID[] partMapping, ToIntBiFunction<Object, Integer> keyMapper) {
            this.keyCfg = keyCfg;
            this.partMapping = partMapping;
            parts = partMapping == null ? null : partMapping.length;
            this.keyMapper = keyMapper;
        }

        /**
         * Calculates node for given key.
         *
         * @param key Key.
         */
        private UUID nodeForKey(Object key) {
            if (parts == null || keyMapper == null)
                return null;

            return nodeForPartition(keyMapper.applyAsInt(key, parts));
        }

        /**
         * Calculates node for given partition.
         *
         * @param part Partition.
         */
        private UUID nodeForPartition(int part) {
            if (part < 0 || partMapping == null || part >= partMapping.length)
                return null;

            return partMapping[part];
        }
    }

    /** Default implementation of cache key to partition mapper. */
    private static class RendezvousAffinityKeyMapper implements ToIntBiFunction<Object, Integer> {
        /** Number of partitions. */
        private final int parts;

        /** Affinity mask. */
        private final int affinityMask;

        /**
         * @param parts Number of partitions.
         */
        private RendezvousAffinityKeyMapper(int parts) {
            this.parts = parts;
            affinityMask = RendezvousAffinityFunction.calculateMask(parts);
        }

        /** {@inheritDoc} */
        @Override public int applyAsInt(Object key, Integer parts) {
            return RendezvousAffinityFunction.calculatePartition(key, affinityMask, this.parts);
        }
    }
}
