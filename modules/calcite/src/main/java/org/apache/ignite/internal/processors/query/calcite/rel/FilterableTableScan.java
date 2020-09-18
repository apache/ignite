package org.apache.ignite.internal.processors.query.calcite.rel;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.jetbrains.annotations.Nullable;

public class FilterableTableScan extends TableScan {
    /** */
    protected final RexNode cond;

    /** */
    public FilterableTableScan(RelOptCluster cluster, RelTraitSet traitSet,
        List<RelHint> hints, RelOptTable table, @Nullable RexNode cond) {
        super(cluster, traitSet, hints, table);
        this.cond = cond;
    }

    /** */
    public FilterableTableScan(RelInput input) {
        super(input);
        cond = input.getExpression("filters");
    }

    /** */
    public RexNode condition() {
        return cond;
    }

    /** {@inheritDoc} */
    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();

        return this;
    }

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        return explainTerms0(super.explainTerms(pw));
    }

    /** */
    protected RelWriter explainTerms0(RelWriter pw) {
        return pw.itemIf("filters", cond, cond != null);
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double tableRows = table.getRowCount();
        return planner.getCostFactory().makeCost(tableRows, 0, 0);
    }

    /** {@inheritDoc} */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        double rows = table.getRowCount();

        if (cond != null)
            rows *= mq.getSelectivity(this, cond);

        return rows;
    }
}
