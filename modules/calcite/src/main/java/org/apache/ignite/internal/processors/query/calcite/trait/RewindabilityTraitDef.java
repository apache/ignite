package org.apache.ignite.internal.processors.query.calcite.trait;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;

public class RewindabilityTraitDef extends RelTraitDef<RewindabilityTrait> {
    @Override public Class<RewindabilityTrait> getTraitClass() {
        return RewindabilityTrait.class;
    }

    @Override public String getSimpleName() {
        return "rewindability";
    }

    @Override public RelNode convert(RelOptPlanner planner, RelNode rel, RewindabilityTrait toTrait, boolean allowInfiniteCostConverters) {
        return null;
    }

    @Override public boolean canConvert(RelOptPlanner planner, RewindabilityTrait fromTrait, RewindabilityTrait toTrait) {
        return false;
    }

    @Override public RewindabilityTrait getDefault() {
        return RewindabilityTrait.notRewindable();
    }
}
