package org.apache.ignite.internal.processors.query.calcite.rule;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;

public abstract class AbstractIgniteConverterRule<T extends RelNode> extends ConverterRule {
    /** */
    protected AbstractIgniteConverterRule(Class<T> clazz) {
        super(clazz, Convention.NONE, IgniteConvention.INSTANCE, clazz.getName() + "Converter");
    }

    /** {@inheritDoc} */
    @Override public final RelNode convert(RelNode rel) {
        return convert(rel.getCluster().getPlanner(), rel.getCluster().getMetadataQuery(), (T)rel);
    }

    protected abstract PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, T rel);
}
