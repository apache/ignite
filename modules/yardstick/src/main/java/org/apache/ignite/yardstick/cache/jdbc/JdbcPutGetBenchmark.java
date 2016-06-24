package org.apache.ignite.yardstick.cache.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static org.apache.ignite.yardstick.IgniteAbstractBenchmark.nextRandom;

/** JDBC benchmark that performs raw SQL inserts with subsequent selects of fresh records */
public class JdbcPutGetBenchmark extends JdbcAbstractBenchmark {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        boolean success = false;
        int newKey;
        do {
            newKey = nextRandom(args.range());
            try (PreparedStatement stmt = conn.get().prepareStatement("insert into SAMPLE(id) values(?)")) {
                try {
                    stmt.setInt(1, newKey);
                    success = (stmt.executeUpdate() > 0);
                }
                catch (SQLException e) {
                    // No-op.
                }
            }
        } while (!success); // In case we've generated a non-unique id
        try (PreparedStatement stmt = conn.get().prepareStatement("select id from SAMPLE where id = ?")) {
            stmt.setInt(1, newKey);
            ResultSet rs = stmt.executeQuery();
            if (rs.next())
                rs.getInt(1);
            else
                throw new Exception("Failed to retrieve sample [id=" + newKey + "]");
        }
        return true;
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        if (!args.createTempDatabase())
            clearTable("SAMPLE");
        super.tearDown();
    }
}
