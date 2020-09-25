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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/** */
public class FilterableTableScan extends TableScan {
    /** */
    protected final RexNode cond;

    /** */
    private List<RexNode> projections;

    /** */
    private ImmutableBitSet requiredColumns;

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

    /**
     * @return Projections.
     */
    public List<RexNode> projections() {
        return projections;
    }

    /**
     * @param prjs New projections.
     */
    public void projections(List<RexNode> prjs) {
        projections = prjs;
    }

    /**
     * @return Required columns.
     */
    public ImmutableBitSet requiredColumns() {
        return requiredColumns;
    }

    /**
     * @param requiredCols New required columns.
     */
    public void requiredColumns(ImmutableBitSet requiredCols) {
        requiredColumns = requiredCols;
    }

    /** */
    @Override public RelDataType deriveRowType() {
        if (F.isEmpty(requiredColumns()))
            return table.getRowType();

        final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
        final RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();

        int startIdx = 0;

        ImmutableBitSet columns = requiredColumns();

        for (;;) {
            int idx = columns.nextSetBit(startIdx);

            if (idx == -1)
                break;

            startIdx = idx + 1;

            builder.add(fieldList.get(idx));
        }

        return builder.build();
    }
}
