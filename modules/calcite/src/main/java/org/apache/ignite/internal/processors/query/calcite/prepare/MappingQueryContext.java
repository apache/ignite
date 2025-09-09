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

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMetadata;
import org.apache.ignite.internal.processors.query.calcite.metadata.RelMetadataQueryEx;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Query mapping context.
 */
public class MappingQueryContext implements Context {
    /** */
    private final Context parent;

    /** */
    private final UUID locNodeId;

    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private final Map<String, Object> params;

    /** */
    private RelOptCluster cluster;

    /** */
    public MappingQueryContext(
        UUID locNodeId,
        AffinityTopologyVersion topVer
    ) {
        this(null, locNodeId, topVer, null);
    }

    /** */
    public MappingQueryContext(
        BaseQueryContext parent,
        UUID locNodeId,
        AffinityTopologyVersion topVer,
        Map<String, Object> params
    ) {
        this.locNodeId = locNodeId;
        this.topVer = topVer;
        this.parent = parent;
        this.params = !F.isEmpty(params) ? Collections.unmodifiableMap(params) : Collections.emptyMap();
    }

    /** */
    public UUID localNodeId() {
        return locNodeId;
    }

    /** */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** */
    public boolean isLocal() {
        BaseQueryContext qryCtx = unwrap(BaseQueryContext.class);

        return qryCtx != null && qryCtx.isLocal();
    }

    /** */
    public int[] partitions() {
        BaseQueryContext qryCtx = unwrap(BaseQueryContext.class);

        return qryCtx != null ? qryCtx.partitions() : null;
    }

    /** */
    public Map<String, Object> queryParameters() {
        return params;
    }

    /** */
    public IgniteTypeFactory typeFactory() {
        BaseQueryContext qryCtx = unwrap(BaseQueryContext.class);

        return qryCtx != null ? qryCtx.typeFactory() : BaseQueryContext.TYPE_FACTORY;
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

    /** {@inheritDoc} */
    @Override public <C> @Nullable C unwrap(Class<C> aCls) {
        if (aCls == getClass())
            return aCls.cast(this);

        return parent != null ? parent.unwrap(aCls) : null;
    }
}
