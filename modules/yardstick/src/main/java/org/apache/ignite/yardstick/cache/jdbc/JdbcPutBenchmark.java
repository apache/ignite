package org.apache.ignite.yardstick.cache.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import static org.apache.ignite.yardstick.IgniteAbstractBenchmark.nextRandom;

/** JDBC benchmark that performs raw SQL insert */
public class JdbcPutBenchmark extends JdbcAbstractBenchmark {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int newKey = nextRandom(args.range());

        try (PreparedStatement stmt = conn.get().prepareStatement("insert into SAMPLE(id, val) values(?, ?) " +
            "on conflict (id) do update set val=?")) {
            stmt.setInt(1, newKey);
            stmt.setInt(2, newKey);
            stmt.setInt(3, newKey);

            return stmt.executeUpdate() > 0;
        }
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        if (!args.createTempDatabase())
            clearTable("SAMPLE");

        super.tearDown();
    }
}
