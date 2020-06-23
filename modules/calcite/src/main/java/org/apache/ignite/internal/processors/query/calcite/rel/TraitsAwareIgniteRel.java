package org.apache.ignite.internal.processors.query.calcite.rel;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.fixTraits;
import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.inputTraits;

/** */
public interface TraitsAwareIgniteRel extends IgniteRel {
    /** {@inheritDoc} */
    @Override public default RelNode passThrough(RelTraitSet required) {
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits =
            prepareTraitsForPassingThrough(required);

        traits = passThroughCollation(traits);
        traits = passThroughDistribution(traits);
        traits = passThroughRewindability(traits);

        List<RelNode> nodes = createNodes(traits);

        RelOptPlanner planner = getCluster().getPlanner();
        for (int i = 1; i < nodes.size(); i++)
            planner.register(nodes.get(i), this);

        return F.first(nodes);
    }

    /** {@inheritDoc} */
    @Override default Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        throw new RuntimeException(getClass().getName() + "#passThroughTraits() is not implemented.");
    }

    /** {@inheritDoc} */
    @Override public default List<RelNode> derive(List<List<RelTraitSet>> inTraits) {
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits =
            prepareTraitsForDerivation(inTraits);

        traits = deriveCollation(traits);
        traits = deriveDistribution(traits);
        traits = deriveRewindability(traits);

        return createNodes(traits);
    }

    /** {@inheritDoc} */
    @Override default Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        throw new RuntimeException(getClass().getName() + "#deriveTraits() is not implemented.");
    }

    /** {@inheritDoc} */
    @Override public default DeriveMode getDeriveMode() {
        return DeriveMode.OMAKASE;
    }

    /** */
    @NotNull default Collection<Pair<RelTraitSet, List<RelTraitSet>>> prepareTraitsForPassingThrough(
        RelTraitSet required) {
        RelTraitSet out = fixTraits(required);

        return ImmutableList.of(Pair.of(out, Commons.transform(getInputs(), i -> fixTraits(i.getCluster().traitSet()))));
    }

    /** */
    Collection<Pair<RelTraitSet, List<RelTraitSet>>> passThroughRewindability(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits);

    /** */
    Collection<Pair<RelTraitSet, List<RelTraitSet>>> passThroughDistribution(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits);

    /** */
    Collection<Pair<RelTraitSet, List<RelTraitSet>>> passThroughCollation(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits);

    /** */
    @NotNull default Collection<Pair<RelTraitSet, List<RelTraitSet>>> prepareTraitsForDerivation(
        List<List<RelTraitSet>> inTraits) {
        HashSet<Pair<RelTraitSet, List<RelTraitSet>>> traits = U.newHashSet(inTraits.size());

        RelTraitSet out = fixTraits(getCluster().traitSet());

        for (List<RelTraitSet> srcTraits : inputTraits(inTraits))
            traits.add(Pair.of(out, Commons.transform(srcTraits, TraitUtils::fixTraits)));

        return traits;
    }

    /** */
    Collection<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits);

    /** */
    Collection<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits);

    /** */
    Collection<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits);

    /** */
    public default @NotNull List<RelNode> createNodes(Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits) {
        return traits.stream().map(this::createNode).collect(Collectors.toList());
    }

    /** */
    default RelNode createNode(Pair<RelTraitSet, List<RelTraitSet>> traits) {
        return copy(traits.left,
            Commons.transform(Ord.zip(traits.right),
                o -> RelOptRule.convert(getInput(o.i), o.e)));
    }
}
