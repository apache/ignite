package org.apache.ignite.yardstick.cache.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

import static org.apache.ignite.yardstick.IgniteAbstractBenchmark.nextRandom;
import static org.apache.ignite.yardstick.cache.jdbc.JdbcPutBenchmark.createUpsertStatement;

/** JDBC benchmark that performs raw SQL inserts with subsequent selects of fresh records */
public class JdbcPutGetBenchmark extends JdbcAbstractBenchmark {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int newKey = nextRandom(args.range);
        int newVal = nextRandom(args.range);

        try (PreparedStatement stmt = createUpsertStatement(conn.get(), newKey, newVal)) {
            if (stmt.executeUpdate() <= 0)
                return false;
        }

        try (PreparedStatement stmt = conn.get().prepareStatement("select id, val from SAMPLE where id = ?")) {
            stmt.setInt(1, newKey);

            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                rs.getInt(1);
                rs.getInt(2);

                return true;
            }
            else
                return false;
        }
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        if (!args.createTempDatabase())
            clearTable("SAMPLE");

        super.tearDown();
    }
}
