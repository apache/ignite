package org.apache.ignite.internal.processors.query.calcite.metadata;

import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.any;
import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.distribution;

public class IgniteMdCumulativeCost implements MetadataHandler<BuiltInMetadata.CumulativeCost> {
    /** */
    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(
        BuiltInMethod.CUMULATIVE_COST.method, new IgniteMdCumulativeCost());

    /** {@inheritDoc} */
    @Override public MetadataDef<BuiltInMetadata.CumulativeCost> getDef() {
        return BuiltInMetadata.CumulativeCost.DEF;
    }

    /** */
    public RelOptCost getCumulativeCost(RelSubset rel, RelMetadataQuery mq) {
        return VolcanoUtils.bestCost(rel);
    }

    /** */
    public RelOptCost getCumulativeCost(RelNode rel, RelMetadataQuery mq) {
        RelOptCost cost = getNonCumulativeCost(rel, mq);

        if (cost.isInfinite())
            return cost;

        List<RelNode> inputs = rel.getInputs();
        for (RelNode input : inputs)
            cost = cost.plus(mq.getCumulativeCost(input));

        return cost;
    }

    /** */
    public RelOptCost getNonCumulativeCost(RelNode rel, RelMetadataQuery mq) {
        RelOptCost cost = mq.getNonCumulativeCost(rel);

        if (cost.isInfinite())
            return cost;

        RelOptCostFactory factory = rel.getCluster().getPlanner().getCostFactory();

        if (distribution(rel) == any() || rel.getConvention() == Convention.NONE)
            return factory.makeInfiniteCost();

        return factory.makeZeroCost().isLt(cost) ? cost : factory.makeTinyCost();
    }
}
