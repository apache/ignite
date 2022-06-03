package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.math.BigDecimal;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;

/** Extracts number of index records. */
public class IndexCountNode<Row> extends AbstractNode<Object[]> implements Downstream<Object[]> {
    /** The index. */
    private final InlineIndex idx;

    /** Ctor. */
    public IndexCountNode(InlineIndex idx, ExecutionContext ctx) {
        super(ctx, ctx.getTypeFactory().createSqlType(SqlTypeName.BIGINT));

        this.idx = idx;
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Object[]> requestDownstream(int idx) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        if (rowsCnt > 1) {
            downstream().push(new Object[] {BigDecimal.valueOf(idx.totalCount())});

            downstream().end();
        }
    }

    /** {@inheritDoc} */
    @Override public void push(Object[] row) throws Exception {
        // Throw 'unsupported'.
        end();
    }

    /** {@inheritDoc} */
    @Override public void end() throws Exception {
        throw new UnsupportedOperationException("IndexCount cannot accept any rows.");
    }
}
