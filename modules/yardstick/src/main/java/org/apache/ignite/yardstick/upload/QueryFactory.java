/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.yardstick.upload;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Factory that hides all test data details:
 * what query to use to create table
 * or what random arguments to set in prepared statement.
 */
public class QueryFactory {
    /** Query to drop table if it exists. */
    private final String dropTableIfExists = "DROP TABLE IF EXISTS test_upload;";

    /** Number of "values" fields in the test table (any field except primary key). */
    private int valFieldsCnt = 10;

    /** Create table. */
    private String createTable = newCreateTableQuery();

    /** Parametrised query to insert new row. */
    private String insert = newInsertQuery();

    /** Query to count table size */
    private String count = "SELECT COUNT(id) FROM test_upload;";

    /** Deletes all rows from the test table. Does NOT drop the table itself */
    private String deleteAll = "DELETE FROM test_upload WHERE id > 0";

    /** Turns on Write Ahead Log. */
    private String turnOnWal = "ALTER TABLE test_upload LOGGING";

    /** Turns off Write Ahead Log. */
    private String turnOffWal = "ALTER TABLE test_upload NOLOGGING";

    /** see {@link #createTable}. */
    private String newCreateTableQuery() {
        StringBuilder create = new StringBuilder("CREATE TABLE test_upload (id LONG PRIMARY KEY");

        for (int vi = 1; vi <= valFieldsCnt; vi++) {
            create.append(", val_").append(vi);

            if (vi % 2 == 1)
                create.append(" VARCHAR(255)");
            else
                create.append(" LONG");

        }

        create.append(");");

        return create.toString();
    }

    /** see {@link #insert}. */
    private String newInsertQuery() {
        StringBuilder insert = new StringBuilder("INSERT INTO test_upload VALUES (?");
        for (int vi = 1; vi <= valFieldsCnt; vi++)
            insert.append(", ?");

        insert.append(");");
        return insert.toString();
    }

    /** see {@link #createTable}. */
    public String createTable() {
        return createTable;
    }

    /** Drops the test table. */
    public String dropTableIfExists() {
        return dropTableIfExists;
    }

    /** see {@link #insert} */
    public String insert() {
        return insert;
    }

    /** see {@link #count} */
    public String count() {
        return count;
    }

    /** see {@link #deleteAll}*/
    public String deleteAll() {
        return deleteAll;
    }

    /**
     * @param csvFilePath path to csv file
     * @return sql query that inserts data from specified csv file
     */
    public String copyFrom(String csvFilePath) {
        return "COPY FROM \"" + csvFilePath + "\" INTO test_upload " + attributes() + " FORMAT CSV;";
    }

    /**
     * Creates string - comma-separated attributes of test table, surrounded with braces
     * Is used as a part of sql statement
     *
     * @return attributes list of test table as part of sql statement
     */
    private String attributes() {
        StringBuilder attrs = new StringBuilder("(id");

        for (int vi = 1; vi <= valFieldsCnt; vi++)
            attrs.append(", val_").append(vi);

        attrs.append(')');

        return attrs.toString();
    }

    /**
     * Fills specified prepared statement with random values and specified id (primary key).
     *
     * @param stmt prepared statement, built from {@link #insert} query.
     * @param id id in the test table.
     * @throws SQLException if statement is not correct.
     */
    public void setRandomInsertArgs(PreparedStatement stmt, long id) throws SQLException {
        stmt.setLong(1, id);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int vi = 1; vi <= valFieldsCnt; vi++) {
            // vi is value index (among all values), but we also have "id" which is primary key
            // so index in query is value index shifted by 1
            int qryIdx = vi + 1;

            long nextVal = rnd.nextLong();

            if (vi % 2 == 1)
                stmt.setLong(qryIdx, nextVal);
            else
                // todo: it's possible to pre-generate values
                stmt.setString(qryIdx, String.valueOf(nextVal));
        }
    }

    /**
     * Generates CSV line containing specified id and random values.
     * This line corresponds 1 row of the test table,
     * which will be inserted in the end.
     *
     * @param id key in the test table
     * @return generated comma-separated line
     */
    public String randomCsvLine(long id) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        StringBuilder line = new StringBuilder().append(id);

        for (int vi = 1; vi <= valFieldsCnt; vi++) {
            line.append(',');

            if (vi % 2 == 1)
                line.append(rnd.nextLong());
            else
                line.append('"').append(rnd.nextLong()).append('"');
        }

        return line.toString();
    }

    /** see {@link #turnOnWal}*/
    public String turnOnWal() {
        return turnOnWal;
    }

    /** see {@link #turnOffWal} */
    public String turnOffWal() {
        return turnOffWal;
    }
}
