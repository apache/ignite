package org.apache.ignite.internal.processors.query.h2;

import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryContext;
import org.apache.ignite.internal.util.GridStripedPool;
import org.h2.engine.Session;

/**
 * Connection pool for H2 indexing.
 */
public final class H2ConnectionPool extends GridStripedPool<H2Connection, SQLException> {
    /** */
    private static final ConcurrentMap<Session,GridH2QueryContext> sesLocQctx = new ConcurrentHashMap<>();

    /** */
    private final String dbUrl;

    /**
     * @param dbUrl Database url.
     */
    public H2ConnectionPool(String dbUrl) {
        super(32, 4);

        this.dbUrl = dbUrl;
    }

    /**
     * @param s Session.
     * @return Session local query context.
     */
    public static GridH2QueryContext queryContext(Session s) {
        assert s != null;

        return sesLocQctx.get(s);
    }

    /** {@inheritDoc} */
    @Override protected boolean validate(H2Connection o) throws SQLException {
        return o.isValid();
    }

    /** {@inheritDoc} */
    @Override protected H2Connection create() throws SQLException {
        return new H2Connection(dbUrl);
    }

    /** {@inheritDoc} */
    @Override protected void cleanup(H2Connection o) throws SQLException {
        sesLocQctx.remove(o.session());
    }

    /** {@inheritDoc} */
    @Override protected void destroy(H2Connection o) throws SQLException {
        o.close();
    }
}
