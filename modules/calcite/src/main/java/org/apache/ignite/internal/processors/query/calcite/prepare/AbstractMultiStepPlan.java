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
import java.util.UUID;

import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.OptimisticPlanningException;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public abstract class AbstractMultiStepPlan implements MultiStepPlan {
    /** */
    protected final List<Fragment> fragments;

    /** */
    protected final List<GridQueryFieldMetadata> fieldsMeta;

    /** */
    protected Map<Long, NodesMapping> mappings;

    /** */
    protected AbstractMultiStepPlan(List<Fragment> fragments, List<GridQueryFieldMetadata> fieldsMeta) {
        this.fragments = fragments;
        this.fieldsMeta = fieldsMeta;
    }

    /** {@inheritDoc} */
    @Override public List<Fragment> fragments() {
        return fragments;
    }

    /** {@inheritDoc} */
    @Override public List<GridQueryFieldMetadata> fieldsMetadata() {
        return fieldsMeta;
    }

    /** {@inheritDoc} */
    @Override public NodesMapping fragmentMapping(Fragment fragment) {
        return fragmentMapping(fragment.fragmentId());
    }

    /** {@inheritDoc} */
    @Override public NodesMapping targetMapping(Fragment fragment) {
        if (fragment.local())
            return null;

        return fragmentMapping(((IgniteSender)fragment.root()).targetFragmentId());
    }

    /** {@inheritDoc} */
    @Override public Map<Long, List<UUID>> remoteSources(Fragment fragment) {
        List<IgniteReceiver> remotes = fragment.remotes();

        if (F.isEmpty(remotes))
            return null;

        HashMap<Long, List<UUID>> res = U.newHashMap(remotes.size());

        for (IgniteReceiver remote : remotes)
            res.put(remote.exchangeId(), fragmentMapping(remote.sourceFragmentId()).nodes());

        return res;
    }

    /** {@inheritDoc} */
    @Override public void init(MappingService mappingService, PlanningContext ctx) {
        mappings = U.newHashMap(fragments.size());

        RelMetadataQuery mq = F.first(fragments).root().getCluster().getMetadataQuery();

        for (int i = 0, j = 0; i < fragments.size();) {
            Fragment fragment = fragments.get(i);

            try {
                mappings.put(fragment.fragmentId(), fragment.map(mappingService, ctx, mq));

                i++;
            }
            catch (OptimisticPlanningException e) {
                if (++j > 3)
                    throw new IgniteSQLException("Failed to map query.", e);

                replace(fragment, new FragmentSplitter(e.node()).go(fragment));

                // restart init routine.
                mappings.clear();
                i = 0;
            }
        }
    }

    /** */
    private NodesMapping fragmentMapping(long fragmentId) {
        return mappings == null ? null : mappings.get(fragmentId);
    }

    /** */
    private void replace(Fragment fragment, List<Fragment> replacement) {
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
