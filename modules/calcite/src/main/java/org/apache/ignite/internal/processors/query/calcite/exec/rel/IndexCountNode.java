package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.math.BigDecimal;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;

/** Sums numbers of the index records. */
public class IndexCountNode<Row> extends AbstractNode<Row> implements Downstream<Row> {
    /** The index. */
    private final InlineIndexImpl idx;

    /** Row factory. */
    private final RowHandler.RowFactory<Row> rowFactory;

    /** Ctor. */
    public IndexCountNode(InlineIndexImpl idx, RowHandler.RowFactory<Row> rowFactory, ExecutionContext<Row> ctx) {
        super(ctx, ctx.getTypeFactory().createSqlType(SqlTypeName.BIGINT));

        this.idx = idx;
        this.rowFactory = rowFactory;
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
            downstream().push(rowFactory.create(BigDecimal.valueOf(idx.totalCount())));

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
