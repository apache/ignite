package org.apache.ignite.internal.processors.query.calcite.trait;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;

public class RewindabilityTraitDef extends RelTraitDef<RewindabilityTrait> {
    /** */
    public static final RewindabilityTraitDef INSTANCE = new RewindabilityTraitDef();

    /** {@inheritDoc} */
    @Override public Class<RewindabilityTrait> getTraitClass() {
        return RewindabilityTrait.class;
    }

    /** {@inheritDoc} */
    @Override public String getSimpleName() {
        return "rewindability";
    }

    /** {@inheritDoc} */
    @Override public RelNode convert(RelOptPlanner planner, RelNode rel, RewindabilityTrait toTrait, boolean allowInfiniteCostConverters) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean canConvert(RelOptPlanner planner, RewindabilityTrait fromTrait, RewindabilityTrait toTrait) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public RewindabilityTrait getDefault() {
        return RewindabilityTrait.NOT_REWINDABLE;
    }
}
