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

package org.apache.ignite.internal.commandline.cache.distribution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopologyImpl;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Collect information on the distribution of partitions.
 */
public class CacheDistributionTask extends VisorMultiNodeTask<CacheDistributionTaskArg,
    CacheDistributionTaskResult, CacheDistributionNode> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Nullable @Override protected CacheDistributionTaskResult reduce0(
        List<ComputeJobResult> list) throws IgniteException {
        Map<UUID, Exception> exceptions = new HashMap<>();
        List<CacheDistributionNode> infos = new ArrayList<>();

        for (ComputeJobResult res : list) {
            if (res.getException() != null)
                exceptions.put(res.getNode().id(), res.getException());
            else
                infos.add(res.getData());
        }

        return new CacheDistributionTaskResult(infos, exceptions);
    }

    /** {@inheritDoc} */
    @Override protected VisorJob<CacheDistributionTaskArg, CacheDistributionNode> job(CacheDistributionTaskArg arg) {
        return new CacheDistributionJob(arg, debug);
    }

    /** Job for node. */
    private static class CacheDistributionJob extends VisorJob<CacheDistributionTaskArg, CacheDistributionNode> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Argument.
         * @param debug Debug.
         */
        public CacheDistributionJob(@Nullable CacheDistributionTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override public CacheDistributionNode run(CacheDistributionTaskArg arg) throws IgniteException {
            try {
                final CacheDistributionNode info = new CacheDistributionNode();

                final ClusterNode node = ignite.localNode();

                info.setNodeId(node.id());
                info.setAddresses(node.addresses().toString());

                if (arg.getUserAttributes() != null) {
                    info.setUserAttributes(new TreeMap<>());

                    for (String userAttribute : arg.getUserAttributes())
                        info.getUserAttributes().put(userAttribute, (String)node.attributes().get(userAttribute));
                }

                info.setGroups(new ArrayList<>());

                Set<Integer> grpIds = new HashSet<>();

                if (arg.getCaches() == null) {
                    final Collection<CacheGroupContext> ctxs = ignite.context().cache().cacheGroups();

                    for (CacheGroupContext ctx : ctxs)
                        grpIds.add(ctx.groupId());
                }
                else {
                    for (String cacheName : arg.getCaches())
                        grpIds.add(CU.cacheId(cacheName));
                }

                if (grpIds.isEmpty())
                    return info;

                for (Integer id : grpIds) {
                    final CacheDistributionGroup grp = new CacheDistributionGroup();

                    info.getGroups().add(grp);

                    grp.setGroupId(id);

                    final DynamicCacheDescriptor desc = ignite.context().cache().cacheDescriptor(id);

                    final CacheGroupContext grpCtx = ignite.context().cache().cacheGroup(desc == null ? id : desc.groupId());

                    grp.setGroupName(grpCtx.cacheOrGroupName());

                    grp.setPartitions(new ArrayList<>());

                    GridDhtPartitionTopologyImpl top = (GridDhtPartitionTopologyImpl)grpCtx.topology();

                    final AffinityAssignment assignment = grpCtx.affinity().readyAffinity(top.readyTopologyVersion());

                    List<GridDhtLocalPartition> locParts = top.localPartitions();

                    for (int i = 0; i < locParts.size(); i++) {
                        GridDhtLocalPartition part = locParts.get(i);

                        if (part == null)
                            continue;

                        final CacheDistributionPartition partDto = new CacheDistributionPartition();

                        grp.getPartitions().add(partDto);

                        int p = part.id();
                        partDto.setPartition(p);
                        partDto.setPrimary(assignment.primaryPartitions(node.id()).contains(p));
                        partDto.setState(part.state());
                        partDto.setUpdateCounter(part.updateCounter());
                        partDto.setSize(desc == null ? part.dataStore().fullSize() : part.dataStore().cacheSize(id));
                    }
                }
                return info;
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CacheDistributionJob.class, this);
        }
    }
}
