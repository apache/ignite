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
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.OptimisticPlanningException;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.util.typedef.F;

/** */
public class QueryMappings {
    /** */
    private static class Mapping {
        /** */
        private final AffinityTopologyVersion ver;

        /** */
        private final Map<Long, NodesMapping> nodeMappings;

        /** */
        private Mapping(AffinityTopologyVersion ver, Map<Long, NodesMapping> nodeMappings) {
            this.ver = ver;
            this.nodeMappings = nodeMappings;
        }
    }

    /** */
    private final AtomicReference<Mapping> mapping = new AtomicReference<>();

    /** */
    public Map<Long, NodesMapping> map(MappingService mappingService, PlanningContext ctx, List<Fragment> fragments) {
        Mapping mapping = this.mapping.get();

        if (mapping != null && Objects.equals(mapping.ver, ctx.topologyVersion()))
            return mapping.nodeMappings;

        ImmutableMap.Builder<Long, NodesMapping> b = ImmutableMap.builder();

        RelMetadataQuery mq = F.first(fragments).root().getCluster().getMetadataQuery();

        boolean save = true;
        for (int i = 0, j = 0; i < fragments.size();) {
            Fragment fragment = fragments.get(i);

            try {
                b.put(fragment.fragmentId(), fragment.map(mappingService, ctx, mq));

                i++;
            }
            catch (OptimisticPlanningException e) {
                save = false; // we mustn't save mappings for mutated fragments

                if (++j > 3)
                    throw new IgniteSQLException("Failed to map query.", e);

                replace(fragments, fragment, new FragmentSplitter(e.node()).go(fragment));

                // restart init routine.
                b = ImmutableMap.builder();
                i = 0;
            }
        }

        ImmutableMap<Long, NodesMapping> mappings = b.build();

        if (save)
            this.mapping.compareAndSet(mapping, new Mapping(ctx.topologyVersion(), mappings));

        return mappings;
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
            else if (!fragment0.local()) {
                IgniteSender sender = (IgniteSender)fragment0.root();
                Long newTargetId = newTargets.get(sender.exchangeId());

                if (newTargetId != null)
                    sender.targetFragmentId(newTargetId);
            }
        }

        fragments.addAll(replacement.subList(1, replacement.size()));
    }
}
