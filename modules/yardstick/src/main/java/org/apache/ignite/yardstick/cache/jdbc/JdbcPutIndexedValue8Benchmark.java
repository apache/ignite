package org.apache.ignite.yardstick.cache.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import static org.apache.ignite.yardstick.IgniteAbstractBenchmark.nextRandom;

/**
 * JDBC benchmark that performs raw SQL insert into a table with 8 btree indexed columns
 */
public class JdbcPutIndexedValue8Benchmark extends JdbcAbstractBenchmark {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        boolean success = false;
        do {
            int newKey = nextRandom(args.range());
            try (PreparedStatement stmt = conn.get().prepareStatement("insert into VALUE8(val1, val2, val3, val4, val5, " +
                "val6, val7, val8) values(?, ?, ?, ?, ?, ?, ?, ?)")) {
                try {
                    for (int i = 0; i < 8; i++)
                        stmt.setInt(i + 1, newKey + i);
                    success = (stmt.executeUpdate() > 0);
                }
                catch (SQLException e) {
                    // No-op
                }
            }
        } while (!success); // In case we've generated a non-unique id
        return true;
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        if (!args.createTempDatabase())
            clearTable("VALUE8");
        super.tearDown();
    }
}
