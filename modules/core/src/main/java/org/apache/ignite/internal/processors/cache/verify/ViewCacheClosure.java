/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.verify;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.datastructures.AtomicDataStructureValue;
import org.apache.ignite.internal.processors.datastructures.DataStructureType;
import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.apache.ignite.internal.processors.datastructures.GridCacheAtomicSequenceValue;
import org.apache.ignite.internal.processors.datastructures.GridCacheInternalKey;
import org.apache.ignite.internal.visor.verify.VisorViewCacheCmd;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * View cache closure.
 */
public class ViewCacheClosure implements IgniteCallable<List<CacheInfo>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Regex. */
    private String regex;

    /** {@code true} to skip cache destroying. */
    private VisorViewCacheCmd cmd;

    @IgniteInstanceResource
    private Ignite ignite;

    /**
     * @param regex Regex name for stopping caches.
     * @param cmd Command.
     */
    public ViewCacheClosure(String regex, VisorViewCacheCmd cmd) {
        this.regex = regex;
        this.cmd = cmd;
    }

    /** {@inheritDoc} */
    @Override public List<CacheInfo> call() throws Exception {
        Pattern compiled = Pattern.compile(regex);

        List<CacheInfo> cacheInfo = new ArrayList<>();

        IgniteKernal k = (IgniteKernal)ignite;

        if (cmd == null)
            cmd = VisorViewCacheCmd.CACHES;

        switch (cmd) {
            case SEQ:
                collectSequences(k.context(), compiled, cacheInfo);

                return cacheInfo;

            case GROUPS:
                Collection<CacheGroupContext> contexts = k.context().cache().cacheGroups();

                for (CacheGroupContext context : contexts) {
                    if (!context.userCache() || !compiled.matcher(context.cacheOrGroupName()).find())
                        continue;

                    CacheInfo ci = new CacheInfo();
                    ci.setGrpName(context.cacheOrGroupName());
                    ci.setGrpId(context.groupId());
                    ci.setCachesCnt(context.caches().size());
                    ci.setPartitions(context.config().getAffinity().partitions());
                    ci.setBackupsCnt(context.config().getBackups());
                    ci.setAffinityClsName(context.config().getAffinity().getClass().getSimpleName());
                    ci.setMode(context.config().getCacheMode());
                    ci.setAtomicityMode(context.config().getAtomicityMode());
                    ci.setMapped(mapped(context.caches().iterator().next().name()));

                    cacheInfo.add(ci);
                }

                return cacheInfo;

            default:
                Map<String, DynamicCacheDescriptor> descMap = k.context().cache().cacheDescriptors();

                for (Map.Entry<String, DynamicCacheDescriptor> entry : descMap.entrySet()) {
                    DynamicCacheDescriptor desc = entry.getValue();

                    if (!desc.cacheType().userCache() || !compiled.matcher(desc.cacheName()).find())
                        continue;

                    CacheInfo ci = new CacheInfo();

                    ci.setCacheName(desc.cacheName());
                    ci.setCacheId(desc.cacheId());
                    ci.setGrpName(desc.groupDescriptor().groupName());
                    ci.setGrpId(desc.groupDescriptor().groupId());
                    ci.setPartitions(desc.cacheConfiguration().getAffinity().partitions());
                    ci.setBackupsCnt(desc.cacheConfiguration().getBackups());
                    ci.setAffinityClsName(desc.cacheConfiguration().getAffinity().getClass().getSimpleName());
                    ci.setMode(desc.cacheConfiguration().getCacheMode());
                    ci.setAtomicityMode(desc.cacheConfiguration().getAtomicityMode());
                    ci.setMapped(mapped(desc.cacheName()));

                    cacheInfo.add(ci);
                }

                return cacheInfo;
        }
    }

    /**
     * @param cacheName Cache name.
     */
    private int mapped(String cacheName) {
        int mapped = 0;

        ClusterGroup srvs = ignite.cluster().forServers();

        Collection<ClusterNode> nodes = srvs.forDataNodes(cacheName).nodes();

        for (ClusterNode node : nodes)
            mapped += ignite.affinity(cacheName).primaryPartitions(node).length;

        return mapped;
    }

    /**
     * @param ctx Context.
     * @param compiled Compiled pattern.
     * @param cacheInfo Cache info.
     */
    private void collectSequences(GridKernalContext ctx, Pattern compiled, List<CacheInfo> cacheInfo) throws IgniteCheckedException {
        String dsCacheName = DataStructuresProcessor.ATOMICS_CACHE_NAME + "@default-ds-group";

        IgniteInternalCache<GridCacheInternalKey, AtomicDataStructureValue> cache0 = ctx.cache().cache(dsCacheName);

        final Iterator<Cache.Entry<GridCacheInternalKey, AtomicDataStructureValue>> iter = cache0.scanIterator(false, null);

        while (iter.hasNext()) {
            Cache.Entry<GridCacheInternalKey, AtomicDataStructureValue> entry = iter.next();

            final AtomicDataStructureValue val = entry.getValue();

            if (val.type() == DataStructureType.ATOMIC_SEQ) {
                final String name = entry.getKey().name();

                if (compiled.matcher(name).find()) {
                    CacheInfo ci = new CacheInfo();
                    ci.setSeqName(name);
                    ci.setSeqVal(((GridCacheAtomicSequenceValue)val).get());

                    cacheInfo.add(ci);
                }

            }
        }
    }
}
