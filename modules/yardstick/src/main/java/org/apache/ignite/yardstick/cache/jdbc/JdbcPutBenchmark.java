package org.apache.ignite.yardstick.cache.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.apache.ignite.IgniteException;

import static org.apache.ignite.yardstick.IgniteAbstractBenchmark.nextRandom;

/** JDBC benchmark that performs raw SQL insert */
public class JdbcPutBenchmark extends JdbcAbstractBenchmark {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int newKey = nextRandom(args.range());

        try (PreparedStatement stmt = createUpsertStatement(conn.get(), newKey)) {
            return stmt.executeUpdate() > 0;
        }
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        if (!args.createTempDatabase())
            clearTable("SAMPLE");

        super.tearDown();
    }

    /**
     * Get upsert statement depending on the type of database
     * @param conn {@link Connection} to get metadata from to determine storage type
     * @param newKey key to insert
     * @return upsert statement with params set
     * @throws SQLException if failed
     */
    static PreparedStatement createUpsertStatement(Connection conn, int newKey) throws SQLException {
        PreparedStatement stmt;
        switch (conn.getMetaData().getDatabaseProductName()) {
            case "H2":
                stmt = conn.prepareStatement("merge into SAMPLE(id, val) values(?, ?)");
                break;
            case "MySQL":
                stmt = conn.prepareStatement("insert into SAMPLE(id, val) values(?, ?) on duplicate key update val = ?");
                stmt.setInt(3, newKey);
                break;
            case "PostgreSQL":
                stmt = conn.prepareStatement("insert into SAMPLE(id, val) values(?, ?) on conflict(id) do " +
                    "update set val = ?");
                stmt.setInt(3, newKey);
                break;
            default:
                throw new IgniteException("Unexpected database type [databaseProductName=" +
                    conn.getMetaData().getDatabaseProductName() + ']');
        }
        stmt.setInt(1, newKey);
        stmt.setInt(2, newKey);
        return stmt;
    }
}
