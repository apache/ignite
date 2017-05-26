package org.apache.ignite.internal.processors.query.h2;

import java.sql.SQLException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.GridStripedPool;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_INDEXING_CONNECTION_POOL_PICK_ATTEMPTS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_INDEXING_CONNECTION_POOL_STRIPES;

/**
 * Connection pool for H2 indexing.
 */
public final class H2ConnectionPool extends GridStripedPool<H2Connection, SQLException> {
    /** */
    private static final int POOL_STRIPES = IgniteSystemProperties.getInteger(
        IGNITE_H2_INDEXING_CONNECTION_POOL_STRIPES,32);

    /** */
    private static final int PICK_ATTEMPTS = IgniteSystemProperties.getInteger(
        IGNITE_H2_INDEXING_CONNECTION_POOL_PICK_ATTEMPTS,4);

    /** */
    private final String dbUrl;

    /**
     * @param dbUrl Database url.
     */
    public H2ConnectionPool(String dbUrl) {
        super(POOL_STRIPES, PICK_ATTEMPTS);

        this.dbUrl = dbUrl;
    }

    /** {@inheritDoc} */
    @Override public H2Connection take() throws SQLException {
        H2Connection c = super.take();

        c.onPoolTake();

        return c;
    }

    /** {@inheritDoc} */
    @Override public void put(H2Connection c) throws SQLException {
        c.onPoolPut();

        super.put(c);
    }

    /** {@inheritDoc} */
    @Override protected boolean validate(H2Connection o) throws SQLException {
        return o.isValid();
    }

    /** {@inheritDoc} */
    @Override protected H2Connection create() throws SQLException {
        return new H2Connection(this, dbUrl);
    }

    /** {@inheritDoc} */
    @Override protected void cleanup(H2Connection o) throws SQLException {
        o.clearSessionLocalQueryContext();
    }

    /** {@inheritDoc} */
    @Override protected void destroy(H2Connection o) throws SQLException {
        o.destroy();
    }
}
