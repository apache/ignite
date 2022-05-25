package org.apache.ignite.internal.processors.query.calcite.exec.rel;

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
        downstream().push(new Long[]{idx.totalCount()});

        downstream().end();
    }

    @Override public void push(Long aLong) throws Exception {
        System.err.println("TEST | push()");
    }

    @Override public void end() throws Exception {
        System.err.println("TEST | end()");
    }
}
