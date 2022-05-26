package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.math.BigDecimal;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;

public class IndexCountNode extends AbstractNode implements Downstream<Long> {
    private final InlineIndexImpl idx;

    public IndexCountNode(InlineIndexImpl idx, ExecutionContext ctx) {
        super(ctx, ctx.getTypeFactory().createSqlType(SqlTypeName.BIGINT));

        this.idx = idx;
    }

    @Override protected void rewindInternal() {
        System.err.println("TEST | rewindInternal()");
    }

    @Override protected Downstream<Long> requestDownstream(int idx) {
        return this;
    }

    @Override public void request(int rowsCnt) throws Exception {
        if (rowsCnt > 1) {
            System.err.println("TEST | IndexCount: totalCnt=" + idx.totalCount());

            downstream().push(new Object[] {BigDecimal.valueOf(idx.totalCount())});

            downstream().end();
        }
    }

    @Override public void push(Long aLong) throws Exception {
        throw new UnsupportedOperationException("IndexCount cannot accept any rows.");
    }

    @Override public void end() throws Exception {
        throw new UnsupportedOperationException("IndexCount cannot accept any rows.");
    }
}
