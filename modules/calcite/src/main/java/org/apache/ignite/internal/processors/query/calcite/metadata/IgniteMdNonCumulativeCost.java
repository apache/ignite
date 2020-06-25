package org.apache.ignite.internal.processors.query.calcite.metadata;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

/** */
public class IgniteMdNonCumulativeCost implements MetadataHandler<BuiltInMetadata.NonCumulativeCost> {
    /** */
    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(
        BuiltInMethod.NON_CUMULATIVE_COST.method, new IgniteMdNonCumulativeCost());

    /** */
    @Override public MetadataDef<BuiltInMetadata.NonCumulativeCost> getDef() {
        return BuiltInMetadata.NonCumulativeCost.DEF;
    }

    /** */
    public RelOptCost getNonCumulativeCost(RelNode rel, RelMetadataQuery mq) {
        return rel.computeSelfCost(rel.getCluster().getPlanner(), mq);
    }
}
