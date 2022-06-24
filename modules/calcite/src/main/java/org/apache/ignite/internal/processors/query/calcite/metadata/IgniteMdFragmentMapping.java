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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMetadata.FragmentMappingMetadata;
import org.apache.ignite.internal.processors.query.calcite.prepare.MappingQueryContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexCount;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableFunctionScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTrimExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteValues;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMethod;

/**
 * Implementation class for {@link RelMetadataQueryEx#fragmentMapping(RelNode, MappingQueryContext)} method call.
 */
public class IgniteMdFragmentMapping implements MetadataHandler<FragmentMappingMetadata> {
    /**
     * Metadata provider, responsible for nodes mapping request. It uses this implementation class under the hood.
     */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            IgniteMethod.FRAGMENT_MAPPING.method(), new IgniteMdFragmentMapping());

    /** {@inheritDoc} */
    @Override public MetadataDef<FragmentMappingMetadata> getDef() {
        return FragmentMappingMetadata.DEF;
    }

    /**
     * Requests meta information about nodes capable to execute a query over particular partitions.
     *
     * @param rel Relational node.
     * @param mq Metadata query instance. Used to request appropriate metadata from node children.
     * @return Nodes mapping, representing a list of nodes capable to execute a query over particular partitions.
     */
    public FragmentMapping fragmentMapping(RelNode rel, RelMetadataQuery mq, MappingQueryContext ctx) {
        throw new AssertionError();
    }

    /**
     * See {@link IgniteMdFragmentMapping#fragmentMapping(RelNode, RelMetadataQuery, MappingQueryContext)}
     */
    public FragmentMapping fragmentMapping(RelSubset rel, RelMetadataQuery mq, MappingQueryContext ctx) {
        throw new AssertionError();
    }

    /**
     * See {@link IgniteMdFragmentMapping#fragmentMapping(RelNode, RelMetadataQuery, MappingQueryContext)}
     */
    public FragmentMapping fragmentMapping(SingleRel rel, RelMetadataQuery mq, MappingQueryContext ctx) {
        return _fragmentMapping(rel.getInput(), mq, ctx);
    }

    /**
     * See {@link IgniteMdFragmentMapping#fragmentMapping(RelNode, RelMetadataQuery, MappingQueryContext)}
     *
     * {@link ColocationMappingException} may be thrown on two children nodes locations merge. This means
     * that the fragment (which part the parent node is) cannot be executed on any node and additional exchange
     * is needed. This case we throw {@link NodeMappingException} with an edge, where we need the additional
     * exchange. After the exchange is put into the fragment and the fragment is split into two ones, fragment meta
     * information will be recalculated for all fragments.
     */
    public FragmentMapping fragmentMapping(BiRel rel, RelMetadataQuery mq, MappingQueryContext ctx) {
        RelNode left = rel.getLeft();
        RelNode right = rel.getRight();

        FragmentMapping fLeft = _fragmentMapping(left, mq, ctx);
        FragmentMapping fRight = _fragmentMapping(right, mq, ctx);

        try {
            return fLeft.colocate(fRight);
        }
        catch (ColocationMappingException e) {
            IgniteExchange lExch = new IgniteExchange(rel.getCluster(), left.getTraitSet(), left, TraitUtils.distribution(left));
            IgniteExchange rExch = new IgniteExchange(rel.getCluster(), right.getTraitSet(), right, TraitUtils.distribution(right));

            RelNode lVar = rel.copy(rel.getTraitSet(), ImmutableList.of(lExch, right));
            RelNode rVar = rel.copy(rel.getTraitSet(), ImmutableList.of(left, rExch));

            RelOptCost lVarCost = mq.getCumulativeCost(lVar);
            RelOptCost rVarCost = mq.getCumulativeCost(rVar);

            if (lVarCost.isLt(rVarCost))
                throw new NodeMappingException("Failed to calculate physical distribution", left, e);
            else
                throw new NodeMappingException("Failed to calculate physical distribution", right, e);
        }
    }

    /**
     * See {@link IgniteMdFragmentMapping#fragmentMapping(RelNode, RelMetadataQuery, MappingQueryContext)}
     *
     * {@link ColocationMappingException} may be thrown on two children nodes locations merge. This means
     * that the fragment (which part the parent node is) cannot be executed on any node and additional exchange
     * is needed. This case we throw {@link NodeMappingException} with an edge, where we need the additional
     * exchange. After the exchange is put into the fragment and the fragment is split into two ones, fragment meta
     * information will be recalculated for all fragments.
     */
    public FragmentMapping fragmentMapping(SetOp rel, RelMetadataQuery mq, MappingQueryContext ctx) {
        FragmentMapping res = null;

        if (TraitUtils.distribution(rel) == IgniteDistributions.random()) {
            for (RelNode input : rel.getInputs())
                res = res == null ? _fragmentMapping(input, mq, ctx) : res.combine(_fragmentMapping(input, mq, ctx));
        }
        else {
            for (RelNode input : rel.getInputs()) {
                try {
                    res = res == null ? _fragmentMapping(input, mq, ctx) : res.colocate(_fragmentMapping(input, mq, ctx));
                }
                catch (ColocationMappingException e) {
                    throw new NodeMappingException("Failed to calculate physical distribution", input, e);
                }
            }
        }

        return res;
    }

    /**
     * See {@link IgniteMdFragmentMapping#fragmentMapping(RelNode, RelMetadataQuery, MappingQueryContext)}
     *
     * Prunes involved partitions (hence nodes, involved in query execution) if possible.
     */
    public FragmentMapping fragmentMapping(IgniteFilter rel, RelMetadataQuery mq, MappingQueryContext ctx) {
        return _fragmentMapping(rel.getInput(), mq, ctx).prune(rel);
    }

    /**
     * See {@link IgniteMdFragmentMapping#fragmentMapping(RelNode, RelMetadataQuery, MappingQueryContext)}
     *
     * Prunes involved partitions (hence nodes, involved in query execution) if possible.
     */
    public FragmentMapping fragmentMapping(IgniteTrimExchange rel, RelMetadataQuery mq, MappingQueryContext ctx) {
        try {
            return FragmentMapping.create(rel.sourceId())
                .colocate(_fragmentMapping(rel.getInput(), mq, ctx));
        }
        catch (ColocationMappingException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * See {@link IgniteMdFragmentMapping#fragmentMapping(RelNode, RelMetadataQuery, MappingQueryContext)}
     */
    public FragmentMapping fragmentMapping(IgniteReceiver rel, RelMetadataQuery mq, MappingQueryContext ctx) {
        return FragmentMapping.create(rel.exchangeId());
    }

    /**
     * See {@link IgniteMdFragmentMapping#fragmentMapping(RelNode, RelMetadataQuery, MappingQueryContext)}
     */
    public FragmentMapping fragmentMapping(IgniteIndexScan rel, RelMetadataQuery mq, MappingQueryContext ctx) {
        return FragmentMapping.create(rel.sourceId(),
            rel.getTable().unwrap(IgniteTable.class).colocationGroup(ctx));
    }

    /**
     * See {@link IgniteMdFragmentMapping#fragmentMapping(RelNode, RelMetadataQuery, MappingQueryContext)}
     */
    public FragmentMapping fragmentMapping(IgniteIndexCount rel, RelMetadataQuery mq, MappingQueryContext ctx) {
        return FragmentMapping.create(rel.sourceId(),
            rel.getTable().unwrap(IgniteTable.class).colocationGroup(ctx));
    }

    /**
     * See {@link IgniteMdFragmentMapping#fragmentMapping(RelNode, RelMetadataQuery, MappingQueryContext)}
     */
    public FragmentMapping fragmentMapping(IgniteTableScan rel, RelMetadataQuery mq, MappingQueryContext ctx) {
        return FragmentMapping.create(rel.sourceId(),
            rel.getTable().unwrap(IgniteTable.class).colocationGroup(ctx));
    }

    /**
     * See {@link IgniteMdFragmentMapping#fragmentMapping(RelNode, RelMetadataQuery, MappingQueryContext)}
     */
    public FragmentMapping fragmentMapping(IgniteValues rel, RelMetadataQuery mq, MappingQueryContext ctx) {
        return FragmentMapping.create();
    }

    /**
     * See {@link IgniteMdFragmentMapping#fragmentMapping(RelNode, RelMetadataQuery, MappingQueryContext)}
     */
    public FragmentMapping fragmentMapping(IgniteTableFunctionScan rel, RelMetadataQuery mq, MappingQueryContext ctx) {
        return FragmentMapping.create();
    }

    /**
     * Fragment info calculation entry point.
     * @param rel Root node of a calculated fragment.
     * @param mq Metadata query instance.
     * @return Fragment meta information.
     */
    public static FragmentMapping _fragmentMapping(RelNode rel, RelMetadataQuery mq, MappingQueryContext ctx) {
        assert mq instanceof RelMetadataQueryEx;

        return ((RelMetadataQueryEx)mq).fragmentMapping(rel, ctx);
    }
}
