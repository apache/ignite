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

/** Scan with projects and filters. */
public class ProjectableFilterableTableScan extends TableScan {
    /** Filters. */
    private final RexNode condition;

    /** Projects. */
    private final List<RexNode> projects;

    /** Participating colunms. */
    private final ImmutableBitSet requiredColunms;

    /** */
    public ProjectableFilterableTableScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelOptTable tbl
    ) {
        this(cluster, traitSet, hints, tbl, null, null, null);
    }

    /** */
    public ProjectableFilterableTableScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelOptTable table,
        @Nullable List<RexNode> proj,
        @Nullable RexNode cond,
        @Nullable ImmutableBitSet reqColunms
    ) {
        super(cluster, traitSet, hints, table);

        projects = proj;
        condition = cond;
        requiredColunms = reqColunms;
    }

    /** */
    public ProjectableFilterableTableScan(RelInput input) {
        super(input);
        condition = input.getExpression("filters");
        projects = input.get("projections") == null ? null : input.getExpressionList("projections");
        requiredColunms = input.get("requiredColunms") == null ? null : input.getBitSet("requiredColunms");
    }

    /** @return Projections. */
    public List<RexNode> projects() {
        return projects;
    }

    /** @return Rex condition. */
    public RexNode condition() {
        return condition;
    }

    /** @return Participating colunms. */
    public ImmutableBitSet requiredColunms() {
        return requiredColunms;
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
            .itemIf("filters", condition, condition != null)
            .itemIf("projections", projects, projects != null);
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double tableRows = table.getRowCount();

        if (projects != null)
            tableRows += tableRows * projects.size();

        return planner.getCostFactory().makeCost(tableRows, 0, 0);
    }

    /** {@inheritDoc} */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        double rows = table.getRowCount();

        if (condition != null)
            rows *= mq.getSelectivity(this, condition);

        return rows;
    }

    /** */
    @Override public RelDataType deriveRowType() {
        if (projects != null)
            return RexUtil.createStructType(Commons.context(this).typeFactory(), projects);

        return table.getRowType();
    }
}
