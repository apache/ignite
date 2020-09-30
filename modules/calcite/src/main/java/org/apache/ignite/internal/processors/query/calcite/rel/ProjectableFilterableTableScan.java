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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.jetbrains.annotations.Nullable;

/** */
public class ProjectableFilterableTableScan extends TableScan {
    /** */
    protected final RexNode cond;

    /** */
    protected final List<RexNode> projections;

    protected final ImmutableBitSet requiredColunms;

    /** */
    public ProjectableFilterableTableScan(RelOptCluster cluster, RelTraitSet traitSet,
        List<RelHint> hints, RelOptTable table) {
        this(cluster, traitSet, hints, table, null, null, null);
    }

    /** */
    public ProjectableFilterableTableScan(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints,
        RelOptTable table, @Nullable List<RexNode> projections, @Nullable RexNode cond, ImmutableBitSet requiredColunms) {
        super(cluster, traitSet, hints, table);

        this.projections = projections;
        this.cond = cond;
        this.requiredColunms = requiredColunms;
    }

    /** */
    public ProjectableFilterableTableScan(RelInput input) {
        super(input);
        cond = input.getExpression("filters");
        projections = input.get("projections") == null ? null : input.getExpressionList("projections");
        requiredColunms = input.get("requiredColunms") == null ? null : input.getBitSet("requiredColunms");
    }

    /**
     * @return Projections.
     */
    public List<RexNode> projections() {
        return projections;
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
        return pw
            .itemIf("filters", cond, cond != null)
            .itemIf("projections", projections, projections != null);
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double tableRows = table.getRowCount();

        if (projections != null)
            tableRows += tableRows * projections.size();

        return planner.getCostFactory().makeCost(tableRows, 0, 0);
    }

    /** {@inheritDoc} */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        double rows = table.getRowCount();

        if (cond != null)
            rows *= mq.getSelectivity(this, cond);

        return rows;
    }

    /** */
    @Override public RelDataType deriveRowType() {
        if (projections != null)
            return RexUtil.createStructType(Commons.context(this).typeFactory(), projections);

        return table.getRowType();
    }

    /**
     * @return Required colunms.
     */
    public ImmutableBitSet requiredColunms() {
        return requiredColunms;
    }
}
