/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.metadata;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.calcite.rel.Receiver;

/**
 *
 */
public class FragmentLocation {
    private NodesMapping mapping;

    private final ImmutableList<Receiver> remoteInputs;
    private final ImmutableIntList localInputs;
    private final AffinityTopologyVersion topVer;

    public FragmentLocation(ImmutableList<Receiver> remoteInputs, AffinityTopologyVersion topVer) {
        this(null, remoteInputs, null, topVer);
    }

    public FragmentLocation(NodesMapping mapping, ImmutableIntList localInputs, AffinityTopologyVersion topVer) {
        this(mapping, null, localInputs, topVer);
    }

    public FragmentLocation(NodesMapping mapping, ImmutableList<Receiver> remoteInputs, ImmutableIntList localInputs, AffinityTopologyVersion topVer) {
        this.mapping = mapping;
        this.remoteInputs = remoteInputs;
        this.localInputs = localInputs;
        this.topVer = topVer;
    }

    public NodesMapping mapping() {
        return mapping;
    }

    public void mapping(NodesMapping mapping) {
        this.mapping = mapping;
    }

    public ImmutableList<Receiver> remoteInputs() {
        return remoteInputs;
    }

    public ImmutableIntList localInputs() {
        return localInputs;
    }

    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }
}
