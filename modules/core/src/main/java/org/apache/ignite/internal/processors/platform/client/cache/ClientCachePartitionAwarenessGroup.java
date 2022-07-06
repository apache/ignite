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

package org.apache.ignite.internal.processors.platform.client.cache;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.platform.client.ClientBitmaskFeature;
import org.apache.ignite.internal.processors.platform.client.ClientProtocolContext;

/**
 * Partition mapping associated with the group of caches.
 */
class ClientCachePartitionAwarenessGroup {
    /** Binary processor. */
    private final CacheObjectBinaryProcessorImpl proc;

    /** Partition mapping. */
    private final ClientCachePartitionMapping mapping;

    /** {@code true} if the default RendezvousAffinityFunction and CacheDefaultBinaryAffinityKeyMapper is used for cache. */
    private final boolean isDefaultAffinity;

    /** Descriptor of the associated caches. */
    private final HashMap<Integer, CacheConfiguration<?, ?>> cacheCfgs;

    /**
     * @param proc Binary processor.
     * @param mapping Partition mapping.
     * @param cacheDesc Descriptor of the initial cache.
     * @param isDefaultAffinity {@code true} if the default RendezvousAffinityFunction and CacheDefaultBinaryAffinityKeyMapper is used for
     * cache.
     */
    public ClientCachePartitionAwarenessGroup(
        CacheObjectBinaryProcessorImpl proc,
        ClientCachePartitionMapping mapping,
        DynamicCacheDescriptor cacheDesc,
        boolean isDefaultAffinity
    ) {
        this.proc = proc;
        this.mapping = mapping;
        this.isDefaultAffinity = isDefaultAffinity;

        int cacheId = cacheDesc.cacheId();
        CacheConfiguration<?, ?> ccfg = cacheDesc.cacheConfiguration();

        cacheCfgs = new HashMap<>();
        cacheCfgs.put(cacheId, ccfg);
    }

    /**
     * Check if the mapping is compatible to a mapping of the group.
     * @param mapping Affinity mapping.
     * @return True if compatible.
     */
    public boolean isCompatible(ClientCachePartitionMapping mapping) {
        // All unapplicable caches go to the same single group, so they are all compatible one to another.
        if (this.mapping == null || mapping == null)
            return this.mapping == mapping;

        // Now we need to compare mappings themselves.
        return mapping.isCompatible(mapping);
    }

    /**
     * Write mapping using binary writer.
     * @param writer Writer.
     */
    public void write(BinaryRawWriter writer, ClientProtocolContext cpctx) {
        writer.writeBoolean(mapping != null);

        writer.writeInt(cacheCfgs.size());

        for (Map.Entry<Integer, CacheConfiguration<?, ?>> entry: cacheCfgs.entrySet()) {
            writer.writeInt(entry.getKey());

            if (mapping == null)
                continue;

            CacheConfiguration<?, ?> ccfg = entry.getValue();
            CacheKeyConfiguration[] keyCfgs = ccfg.getKeyConfiguration();

            if (keyCfgs == null) {
                writer.writeInt(0);

                continue;
            }

            writer.writeInt(keyCfgs.length);

            for (CacheKeyConfiguration keyCfg : keyCfgs) {
                int keyTypeId = proc.typeId(keyCfg.getTypeName());
                int affinityKeyFieldId = proc.binaryContext().fieldId(keyTypeId, keyCfg.getAffinityKeyFieldName());

                writer.writeInt(keyTypeId);
                writer.writeInt(affinityKeyFieldId);
            }
        }

        if (mapping != null)
            mapping.write(writer);

        if (cpctx.isFeatureSupported(ClientBitmaskFeature.ALL_AFFINITY_MAPPINGS))
            writer.writeBoolean(isDefaultAffinity);
    }

    /**
     * Add cache to affinity group.
     * @param desc Cache descriptor.
     */
    public void addCache(DynamicCacheDescriptor desc) {
        cacheCfgs.put(desc.cacheId(), desc.cacheConfiguration());
    }
}
