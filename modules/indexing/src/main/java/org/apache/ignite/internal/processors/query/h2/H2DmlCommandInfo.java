package org.apache.ignite.internal.processors.query.h2;

import java.util.UUID;
import org.apache.ignite.internal.processors.query.running.TrackableQuery;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/** */
public class H2DmlCommandInfo implements TrackableQuery {
    /** Begin timestamp. */
    private final long beginTs;

    /** Query id. */
    private final long qryId;

    private final boolean loc;

    /** Local node id. */
//    private final UUID locNodeId;

    /** Schema name. */
    private final String schema;

    /** Sql text. */
    private final String sql;

//    /** */
//    private final String plan;

    /**
     * @param beginTs Begin ts.
     * @param qryId Query id.
     * @param schema Schema.
     * @param sql Sql.
     */
    public H2DmlCommandInfo(long beginTs, long qryId, boolean loc, String schema, String sql) {
        this.beginTs = beginTs;
        this.qryId = qryId;
        this.loc = loc;
//        this.locNodeId = locNodeId;
        this.schema = schema;
        this.sql = sql;
    }

    /** {@inheritDoc} */
    @Override public long time() {
        return U.currentTimeMillis() - beginTs;
    }

    @Override public String queryInfo(@Nullable String additinalInfo) {
        return "!!!!!!!!";
    }
}
