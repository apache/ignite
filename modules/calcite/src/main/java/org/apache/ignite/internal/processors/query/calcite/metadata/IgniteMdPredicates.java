package org.apache.ignite.internal.processors.query.calcite.metadata;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdPredicates;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;

public class IgniteMdPredicates extends RelMdPredicates {
    /** */
    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
        .reflectiveSource(BuiltInMethod.PREDICATES.method, new IgniteMdPredicates());

    /**
     * See {@link RelMdPredicates#getPredicates(org.apache.calcite.rel.RelNode, org.apache.calcite.rel.metadata.RelMetadataQuery)}
     */
    public RelOptPredicateList getPredicates(IgniteIndexScan rel, RelMetadataQuery mq) {
        if (rel.condition() == null)
            return RelOptPredicateList.EMPTY;

        return RelOptPredicateList.of(rel.getCluster().getRexBuilder(),
                    RexUtil.retainDeterministic(
                        RelOptUtil.conjunctions(rel.condition())));
    }
}
