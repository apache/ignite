package org.apache.ignite.internal.processors.query.calcite.trait;

import com.google.common.collect.Ordering;
import org.apache.calcite.plan.RelMultipleTrait;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;
import org.jetbrains.annotations.NotNull;

public class RewindabilityTrait implements RelMultipleTrait {
    /** */
    private static final Ordering<Iterable<Integer>> ORDERING =
        Ordering.<Integer>natural().lexicographical();

    /** */
    private final ImmutableIntList fields;

    /** */
    private RewindabilityTrait(ImmutableIntList fields) {
        this.fields = fields;
    }

    /** */
    public static RewindabilityTrait notRewindable() {
        return new RewindabilityTrait(null);
    }

    /** */
    public static RewindabilityTrait rewindableBy(int... fields) {
        return new RewindabilityTrait(ImmutableIntList.of(fields));
    }

    /** */
    public boolean rewindable() {
        return fields() != null;
    }

    /** */
    public ImmutableIntList fields() {
        return fields;
    }

    /** {@inheritDoc} */
    @Override public boolean isTop() {
        return !rewindable();
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull RelMultipleTrait o) {
        RewindabilityTrait that = (RewindabilityTrait)o;
        if (this.fields == null || that.fields == null)
            return this.fields == that.fields ? 0 : this.fields == null ? 1 : -1;

        return ORDERING.compare(fields, that.fields);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (!(o instanceof RewindabilityTrait))
            return false;
        return compareTo((RewindabilityTrait)o) == 0;
    }

    /** {@inheritDoc} */
    @Override public RelTraitDef<RewindabilityTrait> getTraitDef() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean satisfies(RelTrait trait) {
        if (this == trait)
            return true;

        if (!(trait instanceof RewindabilityTrait))
            return false;

        RewindabilityTrait trait0 = (RewindabilityTrait)trait;

        if (!trait0.rewindable())
            return true;

        if (!rewindable())
            return false;

        return Util.startsWith(fields, trait0.fields);
    }

    /** {@inheritDoc} */
    @Override public void register(RelOptPlanner planner) {
        // no-op
    }
}
