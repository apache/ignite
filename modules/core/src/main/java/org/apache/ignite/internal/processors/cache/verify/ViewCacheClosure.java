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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.commandline.cache.CacheCommand;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.datastructures.AtomicDataStructureValue;
import org.apache.ignite.internal.processors.datastructures.DataStructureType;
import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.apache.ignite.internal.processors.datastructures.GridCacheAtomicSequenceValue;
import org.apache.ignite.internal.processors.datastructures.GridCacheInternalKey;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

/**
 * View cache closure.
 */
public class ViewCacheClosure implements IgniteCallable<List<CacheInfo>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final String[] args;

    /** Regex. */
    private String regex;

    /** {@code true} to skip cache destroying. */
    private CacheCommand cmd;

    @IgniteInstanceResource
    private Ignite ignite;

    @LoggerResource
    private IgniteLogger logger;

    /**
     * @param regex Regex name for stopping caches.
     * @param cmd Command.
     */
    public ViewCacheClosure(String regex, CacheCommand cmd, String... args) {
        this.regex = regex;
        this.cmd = cmd;
        this.args = args;
    }

    /** {@inheritDoc} */
    @Override public List<CacheInfo> call() throws Exception {
        Pattern compiled = Pattern.compile(regex);

        List<CacheInfo> cacheInfo = new ArrayList<>();

        IgniteKernal k = (IgniteKernal)ignite;

        if (cmd == CacheCommand.SEQ) {
            collectSequences(k.context(), compiled, cacheInfo);

            return cacheInfo;
        }

        if (cmd == CacheCommand.UPDATE_SEQ) {
            if (args == null || args.length == 0) {
                logger.error("Illegal new sequence value: " + Arrays.toString(args));

                return new ArrayList<>(0);
            }

            collectSequences(k.context(), compiled, cacheInfo);

            long newVal = Long.parseLong(args[0]);

            if (newVal < 0) {
                logger.error("Only positive values are supported");

                return new ArrayList<>(0);
            }

            for (CacheInfo info : cacheInfo) {
                final String name = info.getSeqName();

                logger.info("Start processing sequence " + name);

                final IgniteAtomicSequence seq = ignite.atomicSequence(name, 0, false);
                long v1 = seq.get();

                if (v1 >= newVal) {
                    logger.info("Skipping sequence because new value is smaller than current: [seqName=" + name +
                        ", curVal=" + v1 + ", newVal=" + newVal + ']');

                    continue;
                }

                long l0 = -1;
                long l = 0;

                try {
                    final long l1 = 500_000_000_000L;

                    if (v1 < -l1) {
                        // Hack for prom.
                        seq.addAndGet(l1);

                        v1 = v1 + l1;

                        l0 = seq.addAndGet(0 - v1);

                        l = seq.addAndGet(Math.max(0, newVal - seq.batchSize()));
                    }
                    else if (v1 < 0) {
                        // Adjust to 0 to avoid overflow.
                        l0 = seq.addAndGet(0 - v1);

                        l = seq.addAndGet(Math.max(0, newVal - seq.batchSize()));
                    }
                    else
                        l = seq.addAndGet(Math.max(0, newVal - v1 - seq.batchSize()));

                    info.setSeqVal(l);
                }
                catch (Throwable e) {
                    throw new IgniteException("Cannot handle sequence: " + name + ", curVal=" + v1 + ", newVal=" + newVal +
                        ", batchSize=" + seq.batchSize() + ", l0=" + l0, e);
                }

                logger.info("[seqName=" + name + ", curVal=" + l + ']');
            }

            return cacheInfo;
        }

        if (cmd == CacheCommand.DESTROY_SEQ) {
            collectSequences(k.context(), compiled, cacheInfo);

            for (CacheInfo info : cacheInfo) {
                final String name = info.getSeqName();

                final IgniteAtomicSequence seq = ignite.atomicSequence(name, 0, false);

                seq.close();

                logger.info("Destroying sequence: [seqName=" + name + ']');
            }

            return cacheInfo;
        }

        if (cmd == CacheCommand.GROUPS) {
            Collection<CacheGroupContext> contexts = k.context().cache().cacheGroups();

            for (CacheGroupContext context : contexts) {
                CacheInfo ci = new CacheInfo();
                ci.setGrpName(context.name());
                ci.setGrpId(context.groupId());

                cacheInfo.add(ci);
            }

            return cacheInfo;
        }

        Map<String, DynamicCacheDescriptor> descMap = k.context().cache().cacheDescriptors();

        for (Map.Entry<String, DynamicCacheDescriptor> entry : descMap.entrySet()) {
            CacheInfo ci = new CacheInfo();
            ci.setCacheName(entry.getValue().cacheName());
            ci.setCacheId(entry.getValue().cacheId());
            ci.setGrpName(entry.getValue().groupDescriptor().groupName());
            ci.setGrpId(entry.getValue().groupDescriptor().groupId());
            ci.setPartitions(entry.getValue().cacheConfiguration().getAffinity().partitions());
            ci.setBackupsCnt(entry.getValue().cacheConfiguration().getBackups());
            ci.setAffinityClsName(entry.getValue().cacheConfiguration().getAffinity().getClass().getSimpleName());
            ci.setMode(entry.getValue().cacheConfiguration().getCacheMode());

            int mapped = 0;

            ClusterGroup servers = ignite.cluster().forServers();

            Collection<ClusterNode> nodes = servers.forDataNodes(ci.getCacheName()).nodes();

            ci.setPrimary(new HashMap<ClusterNode, int[]>());
            ci.setBackups(new HashMap<ClusterNode, int[]>());

            for (ClusterNode node : nodes) {
                int[] ints = ignite.affinity(ci.getCacheName()).primaryPartitions(node);

                mapped += ints.length;

                if (cmd == CacheCommand.AFFINITY) {
                    ci.getPrimary().put(node, ints);

                    ci.getBackups().put(node, ignite.affinity(ci.getCacheName()).backupPartitions(node));

                    GridCacheAdapter<Object, Object> cache = k.context().cache().internalCache(entry.getValue().cacheName());

                    GridCacheContext<Object, Object> cctx = cache.context();

                    final AffinityTopologyVersion readyTopVer = k.context().cache().context().exchange().readyAffinityVersion();

                    AffinityAssignment assign0 = cctx.affinity().assignment(readyTopVer);

                    ci.setTopologyVersion(readyTopVer);

//                    ci.setAssignment(assign0.assignment());
//
//                    ci.setIdealAssignment(assign0.idealAssignment());
//
//                    ci.setPrimaryMap((Map<UUID, Set<Integer>>)U.field(assign0, "primary"));
                }
            }

            ci.setMapped(mapped);

            if (compiled.matcher(ci.getCacheName()).find())
                cacheInfo.add(ci);
        }

        if (cmd == CacheCommand.DESTROY) {
            ignite.destroyCaches(F.transform(cacheInfo, new IgniteClosure<CacheInfo, String>() {
                @Override public String apply(CacheInfo info) {
                    return info.getCacheName();
                }
            }));

            logger.info("Destroyed caches count: " + cacheInfo.size());
        }

        return cacheInfo;
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
