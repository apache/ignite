package org.apache.ignite.internal.processors.sql;

import java.math.BigDecimal;
import java.util.List;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.testframework.GridTestUtils;

public class IgniteTransactionSQLColumnConstraintTest extends IgniteSQLColumnConstraintsTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);

        runSQL("CREATE TABLE varchar_table(id INT PRIMARY KEY, str VARCHAR(5)) WITH \"atomicity=transactional_snapshot\"");

        execSQL("INSERT INTO varchar_table VALUES(?, ?)", 1, "12345");

        compareSqlResult("SELECT * FROM varchar_table WHERE id = ?", 1, "12345");

        runSQL("CREATE TABLE decimal_table(id INT PRIMARY KEY, val DECIMAL(4, 2)) WITH \"atomicity=transactional_snapshot\"");

        execSQL("INSERT INTO decimal_table VALUES(?, ?)", 1, 12.34);

        compareSqlResult("SELECT * FROM decimal_table WHERE id = ?", 1, BigDecimal.valueOf(12.34));

        runSQL("CREATE TABLE char_table(id INT PRIMARY KEY, str CHAR(5)) WITH \"atomicity=transactional_snapshot\"");

        execSQL("INSERT INTO char_table VALUES(?, ?)", 1, "12345");

        compareSqlResult("SELECT * FROM char_table WHERE id = ?", 1, "12345");

        runSQL("CREATE TABLE decimal_table_4(id INT PRIMARY KEY, field DECIMAL(4, 2)) WITH \"atomicity=transactional_snapshot\"");

        runSQL("CREATE TABLE char_table_2(id INT PRIMARY KEY, field INTEGER) WITH \"atomicity=transactional_snapshot\"");

        runSQL("CREATE TABLE decimal_table_2(id INT PRIMARY KEY, field INTEGER) WITH \"atomicity=transactional_snapshot\"");

        runSQL("CREATE TABLE char_table_3(id INT PRIMARY KEY, field CHAR(5), field2 INTEGER) WITH \"atomicity=transactional_snapshot\"");

        runSQL("CREATE TABLE decimal_table_3(id INT PRIMARY KEY, field DECIMAL(4, 2), field2 INTEGER) WITH \"atomicity=transactional_snapshot\"");

        runSQL("CREATE TABLE char_table_4(id INT PRIMARY KEY, field CHAR(5)) WITH \"atomicity=transactional_snapshot\"");
    }

    /** */
    @Override protected void checkSQLThrows(String sql, String sqlStateCode, Object... args) {
        runSQL("BEGIN TRANSACTION");

        IgniteSQLException err = (IgniteSQLException)GridTestUtils.assertThrowsWithCause(() -> {
            runSQL(sql, args);

            return 0;
        }, IgniteSQLException.class);

        runSQL("ROLLBACK TRANSACTION");

        assertEquals(err.sqlState(), sqlStateCode);
    }

    /** */
    @Override protected List<?> execSQL(String sql, Object... args) {
        runSQL("BEGIN TRANSACTION");

        List<?> res = runSQL(sql, args);

        runSQL("COMMIT TRANSACTION");

        return res;
    }

    /** */
    private List<?> runSQL(String sql, Object... args)  {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql)
            .setArgs(args);

        return grid(0).context().query().querySqlFields(qry, true).getAll();
    }

    @Override protected boolean mvccEnabled() {
        return true;
    }
}
