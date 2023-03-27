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

import java.util.UUID;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMetadata;
import org.apache.ignite.internal.processors.query.calcite.metadata.RelMetadataQueryEx;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * Query mapping context.
 */
public class MappingQueryContext {
    /** */
    private final UUID locNodeId;

    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private RelOptCluster cluster;

    /** */
    public MappingQueryContext(UUID locNodeId, AffinityTopologyVersion topVer) {
        this.locNodeId = locNodeId;
        this.topVer = topVer;
    }

    /** */
    public UUID localNodeId() {
        return locNodeId;
    }

    /** */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** Creates a cluster. */
    RelOptCluster cluster() {
        if (cluster == null) {
            cluster = RelOptCluster.create(Commons.emptyCluster().getPlanner(), Commons.emptyCluster().getRexBuilder());
            cluster.setMetadataProvider(new CachingRelMetadataProvider(IgniteMetadata.METADATA_PROVIDER,
                Commons.emptyCluster().getPlanner()));
            cluster.setMetadataQuerySupplier(RelMetadataQueryEx::create);
        }

        return cluster;
    }
}
