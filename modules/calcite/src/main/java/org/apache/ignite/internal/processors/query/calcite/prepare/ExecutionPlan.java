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
import com.google.common.collect.ImmutableList;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.calcite.exec.partition.PartitionNode;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMapping;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class ExecutionPlan {
    /** */
    private final AffinityTopologyVersion ver;

    /** */
    private final ImmutableList<Fragment> fragments;

    /** */
    private final ImmutableList<PartitionNode> partNodes;

    /** */
    ExecutionPlan(AffinityTopologyVersion ver, List<Fragment> fragments, List<PartitionNode> partNodes) {
        this.ver = ver;
        this.fragments = ImmutableList.copyOf(fragments);
        this.partNodes = ImmutableList.copyOf(partNodes);
    }

    /** */
    public AffinityTopologyVersion topologyVersion() {
        return ver;
    }

    /** */
    public List<Fragment> fragments() {
        return fragments;
    }

    /** */
    public List<PartitionNode> partitionNodes() {
        return partNodes;
    }

    /** */
    public FragmentMapping mapping(Fragment fragment) {
        return fragment.mapping();
    }

    /** */
    public ColocationGroup target(Fragment fragment) {
        if (fragment.rootFragment())
            return null;

        IgniteSender snd = (IgniteSender)fragment.root();
        return mapping(snd.targetFragmentId()).findGroup(snd.exchangeId());
    }

    /** */
    public Map<Long, List<UUID>> remotes(Fragment fragment) {
        List<IgniteReceiver> remotes = fragment.remotes();

        if (F.isEmpty(remotes))
            return null;

        HashMap<Long, List<UUID>> res = U.newHashMap(remotes.size());

        for (IgniteReceiver remote : remotes)
            res.put(remote.exchangeId(), mapping(remote.sourceFragmentId()).nodeIds());

        return res;
    }

    /** */
    private FragmentMapping mapping(long fragmentId) {
        return fragments().stream()
            .filter(f -> f.fragmentId() == fragmentId)
            .findAny().orElseThrow(() -> new IllegalStateException("Cannot find fragment with given ID. [" +
                "fragmentId=" + fragmentId + ", " + "fragments=" + fragments() + "]"))
            .mapping();
    }
}
