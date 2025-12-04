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

package org.apache.ignite.internal.processors.query.calcite.rel;

import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/** */
public class IgniteTableModify extends TableModify implements IgniteRel {
    /** If table modify can affect data source. */
    private final boolean affectsSrc;

    /**
     * Creates a {@code TableModify}.
     *
     * <p>The UPDATE operation has format like this:
     * <blockquote>
     * <pre>UPDATE table SET iden1 = exp1, ident2 = exp2  WHERE condition</pre>
     * </blockquote>
     *
     * @param cluster Cluster this relational expression belongs to.
     * @param traitSet Traits of this relational expression.
     * @param table Target table to modify.
     * @param input Sub-query or filter condition.
     * @param operation Modify operation (INSERT, UPDATE, DELETE, MERGE).
     * @param updateColumnList List of column identifiers to be updated (e.g. ident1, ident2); null if not UPDATE.
     * @param srcExpressionList List of value expressions to be set (e.g. exp1, exp2); null if not UPDATE.
     * @param flattened Whether set flattens the input row type.
     * @param affectsSrc If table modify can affect data source.
     */
    public IgniteTableModify(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        RelNode input,
        Operation operation,
        List<String> updateColumnList,
        List<RexNode> srcExpressionList,
        boolean flattened,
        boolean affectsSrc
    ) {
        super(cluster, traitSet, table, Commons.context(cluster).catalogReader(),
            input, operation, updateColumnList,
            srcExpressionList, flattened);

        this.affectsSrc = affectsSrc;
    }

    /**
     * Creates a {@code TableModify} from serialized {@link RelInput input}.
     *
     * @param input The input to create node from.
     */
    public IgniteTableModify(RelInput input) {
        this(
            input.getCluster(),
            input.getTraitSet().replace(IgniteConvention.INSTANCE),
            input.getTable("table"),
            input.getInput(),
            input.getEnum("operation", Operation.class),
            input.getStringList("updateColumnList"),
            input.getExpressionList("sourceExpressionList"),
            input.getBoolean("flattened", true),
            false // Field only for planning.
        );
    }

    /** {@inheritDoc} */
    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new IgniteTableModify(
            getCluster(),
            traitSet,
            getTable(),
            sole(inputs),
            getOperation(),
            getUpdateColumnList(),
            getSourceExpressionList(),
            isFlattened(),
            affectsSrc);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteTableModify(cluster, getTraitSet(), getTable(), sole(inputs),
            getOperation(), getUpdateColumnList(), getSourceExpressionList(), isFlattened(), affectsSrc);
    }

    /** {@inheritDoc} */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        return 1.0D;
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        // Don't derive traits for single-node table modify.
        if (TraitUtils.distribution(traitSet) == IgniteDistributions.single())
            return null;

        assert childId == 0;

        if (childTraits.getConvention() != IgniteConvention.INSTANCE)
            return null;

        // If modify can affect data source (for example, INSERT contains self table as source) only
        // modified table affinity distibution is possible, otherwise inconsistency is possible on remote nodes.
        if (affectsSrc)
            return null;

        // Any distributed (random/hash) trait is accepted if data source is not affected by modify.
        if (!TraitUtils.distribution(childTraits).satisfies(IgniteDistributions.random()))
            return null;

        return Pair.of(traitSet, ImmutableList.of(childTraits));
    }
}
