package org.apache.ignite.internal.processors.query.calcite.trait;

import org.apache.calcite.plan.RelMultipleTrait;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.jetbrains.annotations.NotNull;

public class RewindabilityTrait implements RelMultipleTrait {
    /** */
    public static final RewindabilityTrait ONE_WAY = canonize(new RewindabilityTrait(false));

    /** */
    public static final RewindabilityTrait REWINDABLE = canonize(new RewindabilityTrait(true));

    /** */
    private final boolean rewindable;

    /** */
    private RewindabilityTrait(boolean rewindable) {
        this.rewindable = rewindable;
    }

    /** */
    public boolean rewindable() {
        return rewindable;
    }

    /** {@inheritDoc} */
    @Override public boolean isTop() {
        return !rewindable();
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull RelMultipleTrait o) {
        RewindabilityTrait that = (RewindabilityTrait)o;
        return Boolean.compare(that.rewindable, rewindable);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (!(o instanceof RewindabilityTrait))
            return false;
        return compareTo((RewindabilityTrait)o) == 0;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return (rewindable ? 1 : 0);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return rewindable ? "rewindable" : "one-way";
    }

    /** {@inheritDoc} */
    @Override public RelTraitDef<RewindabilityTrait> getTraitDef() {
        return RewindabilityTraitDef.INSTANCE;
    }

    /** {@inheritDoc} */
    @Override public boolean satisfies(RelTrait trait) {
        if (this == trait)
            return true;

        if (!(trait instanceof RewindabilityTrait))
            return false;

        RewindabilityTrait trait0 = (RewindabilityTrait)trait;

        return !trait0.rewindable() || rewindable();
    }

    /** {@inheritDoc} */
    @Override public void register(RelOptPlanner planner) {
        // no-op
    }

    /** */
    private static RewindabilityTrait canonize(RewindabilityTrait trait) {
        return RewindabilityTraitDef.INSTANCE.canonize(trait);
    }
}
