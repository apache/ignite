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

package org.apache.ignite.internal.sql.engine.prepare;

import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.sql.engine.metadata.FragmentMappingException;
import org.apache.ignite.internal.sql.engine.metadata.MappingService;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.NotNull;

/**
 * QueryTemplate.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class QueryTemplate {
    private final List<Fragment> fragments;

    private final AtomicReference<ExecutionPlan> executionPlan = new AtomicReference<>();

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public QueryTemplate(List<Fragment> fragments) {
        List<Fragment> frgs = new ArrayList<>(fragments.size());

        RelOptCluster cluster = Commons.createCluster();

        for (Fragment fragment : fragments) {
            frgs.add(fragment.copy(cluster));
        }

        this.fragments = List.copyOf(frgs);
    }

    /**
     * Map.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public ExecutionPlan map(MappingService mappingService, MappingQueryContext ctx) {
        ExecutionPlan executionPlan = this.executionPlan.get();
        if (executionPlan != null && Objects.equals(executionPlan.topologyVersion(), ctx.topologyVersion())) {
            return executionPlan;
        }

        RelOptCluster cluster = Commons.createCluster();

        List<Fragment> fragments = Commons.transform(this.fragments, fragment -> fragment.copy(cluster));

        Exception ex = null;
        RelMetadataQuery mq = first(fragments).root().getCluster().getMetadataQuery();
        for (int i = 0; i < 3; i++) {
            try {
                ExecutionPlan executionPlan0 = new ExecutionPlan(ctx.topologyVersion(), map(mappingService, fragments, ctx, mq));

                if (executionPlan == null || executionPlan.topologyVersion() < executionPlan0.topologyVersion()) {
                    this.executionPlan.compareAndSet(executionPlan, executionPlan0);
                }

                return executionPlan0;
            } catch (FragmentMappingException e) {
                if (ex == null) {
                    ex = e;
                } else {
                    ex.addSuppressed(e);
                }

                fragments = replace(fragments, e.fragment(), new FragmentSplitter(e.node()).go(e.fragment()));
            }
        }

        throw new IgniteException("Failed to map query.", ex);
    }

    @NotNull
    private List<Fragment> map(MappingService mappingService, List<Fragment> fragments, MappingQueryContext ctx, RelMetadataQuery mq) {
        List<Fragment> frgs = new ArrayList<>();

        for (Fragment fragment : fragments) {
            frgs.add(fragment.map(mappingService, ctx, mq));
        }

        return List.copyOf(frgs);
    }

    private List<Fragment> replace(List<Fragment> fragments, Fragment fragment, List<Fragment> replacement) {
        assert !nullOrEmpty(replacement);

        Long2LongOpenHashMap newTargets = new Long2LongOpenHashMap();
        for (Fragment fragment0 : replacement) {
            for (IgniteReceiver remote : fragment0.remotes()) {
                newTargets.put(remote.exchangeId(), fragment0.fragmentId());
            }
        }

        List<Fragment> fragments0 = new ArrayList<>(fragments.size() + replacement.size() - 1);
        for (Fragment fragment0 : fragments) {
            if (fragment0 == fragment) {
                fragment0 = first(replacement);
            } else if (!fragment0.rootFragment()) {
                IgniteSender sender = (IgniteSender) fragment0.root();

                long newTargetId = newTargets.getOrDefault(sender.exchangeId(), Long.MIN_VALUE);

                if (newTargetId != Long.MIN_VALUE) {
                    sender = new IgniteSender(sender.getCluster(), sender.getTraitSet(),
                            sender.getInput(), sender.exchangeId(), newTargetId, sender.distribution());

                    fragment0 = new Fragment(fragment0.fragmentId(), sender, fragment0.remotes());
                }
            }

            fragments0.add(fragment0);
        }

        fragments0.addAll(replacement.subList(1, replacement.size()));

        return fragments0;
    }
}
