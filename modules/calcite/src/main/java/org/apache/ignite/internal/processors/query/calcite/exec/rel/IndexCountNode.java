package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexQueryContext;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;

/** Extracts number of index records. */
public class IndexCountNode<Row> extends AbstractNode<Row> implements Downstream<Row> {
    /** */
    private final InlineIndex idx;

    /** */
    private final IndexQueryContext qryCtx;

    /** Ctor. */
    public IndexCountNode(InlineIndex idx, ExecutionContext<Row> ctx, IndexQueryContext qryCtx) {
        super(ctx, ctx.getTypeFactory().createSqlType(SqlTypeName.BIGINT));

        this.idx = idx;
        this.qryCtx = qryCtx;
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        if (rowsCnt > 1) {
            long cnt = 0;

            if (qryCtx == null)
                cnt = idx.totalCount();
            else {
                for (int i = 0; i < idx.segmentsCount(); ++i)
                    cnt += idx.count(i, qryCtx);
            }

            downstream().push(context().rowHandler().factory(context().getTypeFactory(), rowType()).create(cnt));

            downstream().end();
        }
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) throws Exception {
        // Throw 'unsupported'.
        end();
    }

    /** {@inheritDoc} */
    @Override public void end() throws Exception {
        throw new UnsupportedOperationException("IndexCount cannot accept any rows.");
    }
}
