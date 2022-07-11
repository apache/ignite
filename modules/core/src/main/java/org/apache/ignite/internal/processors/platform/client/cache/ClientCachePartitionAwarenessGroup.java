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
import java.util.Objects;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.platform.client.ClientBitmaskFeature;
import org.apache.ignite.internal.processors.platform.client.ClientProtocolContext;
import static org.apache.ignite.internal.processors.platform.client.cache.ClientCachePartitionsRequest.isDefaultAffinity;

/**
 * Partition mapping associated with the group of caches.
 */
class ClientCachePartitionAwarenessGroup {
    /** Partition mapping. */
    private final ClientCachePartitionMapping mapping;

    /** {@code true} if the RendezvousAffinityFunction is used with the default affinity key mapper. */
    private final boolean dfltAffinity;

    /** Descriptor of the associated caches. */
    private final Map<Integer, CacheConfiguration<?, ?>> cacheCfgs = new HashMap<>();

    /**
     * @param mapping Partition mapping.
     * @param cacheDesc Descriptor of the initial cache.
     */
    public ClientCachePartitionAwarenessGroup(ClientCachePartitionMapping mapping, DynamicCacheDescriptor cacheDesc) {
        this.mapping = mapping;

        dfltAffinity = isDefaultAffinity(cacheDesc.cacheConfiguration());
        cacheCfgs.put(cacheDesc.cacheId(), cacheDesc.cacheConfiguration());
    }

    /**
     * Write mapping using binary writer.
     *
     * @param proc Binary processor.
     * @param writer Binary Writer.
     * @param cpctx Protocol context.
     */
    public void write(CacheObjectBinaryProcessorImpl proc, BinaryRawWriter writer, ClientProtocolContext cpctx) {
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
            writer.writeBoolean(dfltAffinity);
    }

    /**
     * Add cache to affinity group.
     * @param desc Cache descriptor.
     */
    public void addCache(DynamicCacheDescriptor desc) {
        cacheCfgs.putIfAbsent(desc.cacheId(), desc.cacheConfiguration());
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ClientCachePartitionAwarenessGroup group = (ClientCachePartitionAwarenessGroup)o;

        return dfltAffinity == group.dfltAffinity && Objects.equals(mapping, group.mapping);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(mapping, dfltAffinity);
    }
}
