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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMappingException;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;

/** */
public class QueryMappings {
    /** */
    private static class Mapping {
        /** */
        private final AffinityTopologyVersion ver;

        /** */
        private final Map<Long, FragmentMapping> nodeMappings;

        /** */
        private Mapping(AffinityTopologyVersion ver, Map<Long, FragmentMapping> nodeMappings) {
            this.ver = ver;
            this.nodeMappings = nodeMappings;
        }
    }

    /** */
    private final AtomicReference<Mapping> mapping = new AtomicReference<>();

    /** */
    public Map<Long, FragmentMapping> map(MappingService mappingService, PlanningContext ctx, List<Fragment> fragments) {
        Mapping mapping = this.mapping.get();
        if (mapping != null && Objects.equals(mapping.ver, ctx.topologyVersion()))
            return mapping.nodeMappings;

        Exception ex = null;
        RelMetadataQuery mq = F.first(fragments).root().getCluster().getMetadataQuery();
        for (int i = 0; i < 3; i++) {
            try {
                Map<Long, FragmentMapping> res = finalize(mappingService, ctx, fragments, Commons.transform(fragments, f -> f.map(ctx, mq)));

                if (i == 0) // TODO at this point we don't allow to cache some queries running on a client
                    this.mapping.compareAndSet(mapping, new Mapping(ctx.topologyVersion(), res));

                return res;
            }
            catch (FragmentMappingException e) {
                if (ex == null)
                    ex = e;
                else
                    ex.addSuppressed(e);

                replace(fragments, e.fragment(), new FragmentSplitter(e.node()).go(e.fragment()));
            }
        }

        throw new IgniteSQLException("Failed to map query.", ex);
    }

    private Map<Long, FragmentMapping> finalize(MappingService mappingService, PlanningContext ctx, List<Fragment> fragments, List<FragmentMapping> mappings) {
        AffinityTopologyVersion topVer = ctx.topologyVersion();

        ImmutableMap.Builder<Long, FragmentMapping> b = ImmutableMap.builder();
        for (Pair<Fragment, FragmentMapping> p : Pair.zip(fragments, mappings))
            b.put(p.left.fragmentId(),
                p.right.finalize(
                    () -> mappingService.executionNodes(topVer, single(p.left.root()), null)));

        return b.build();
    }

    /** */
    private boolean single(IgniteRel root) {
        return root instanceof IgniteSender
            && ((IgniteSender)root).sourceDistribution().satisfies(IgniteDistributions.single());
    }

    /** */
    private void replace(List<Fragment> fragments, Fragment fragment, List<Fragment> replacement) {
        assert !F.isEmpty(replacement);

        Map<Long, Long> newTargets = new HashMap<>();

        for (Fragment fragment0 : replacement) {
            for (IgniteReceiver remote : fragment0.remotes())
                newTargets.put(remote.exchangeId(), fragment0.fragmentId());
        }

        for (int i = 0; i < fragments.size(); i++) {
            Fragment fragment0 = fragments.get(i);

            if (fragment0 == fragment)
                fragments.set(i, F.first(replacement));
            else if (!fragment0.rootFragment()) {
                IgniteSender sender = (IgniteSender)fragment0.root();
                Long newTargetId = newTargets.get(sender.exchangeId());

                if (newTargetId != null)
                    sender.targetFragmentId(newTargetId);
            }
        }

        fragments.addAll(replacement.subList(1, replacement.size()));
    }
}
