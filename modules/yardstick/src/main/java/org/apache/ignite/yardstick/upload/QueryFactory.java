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

public class QueryFactory {
    private int valFieldsCnt = 10;

    private String createTable = newCreateTableQuery();

    private String insert = newInsertQuery();

    private String count = "SELECT COUNT(id) FROM test_upload;";

    private String deleteAll = "DELETE FROM test_upload WHERE id > 0";

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

    private String newInsertQuery() {
        StringBuilder insert = new StringBuilder("INSERT INTO test_upload VALUES (?");
        for (int vi = 1; vi <= valFieldsCnt; vi++)
            insert.append(", ?");

        insert.append(");");
        return insert.toString();
    }

    /**
     * @return N fields.
     */
    public int valFieldsCnt() {
        return valFieldsCnt;
    }

    /**
     * @return Create table.
     */
    public String createTable() {
        return createTable;
    }

    /**
     * @return Insert.
     */
    public String insert() {
        return insert;
    }

    /** */
    public String count(){
        return count;
    }

    public String deleteAll(){
        return deleteAll;
    }

    /**
     * @param csvFilePath path to csv file
     * @return sql query that inserts data from specified csv file
     */
    public String copyFrom(String csvFilePath){
        return "COPY FROM \"" + csvFilePath + "\" INTO test_upload " + attributes() + " FORMAT CSV;";
    }

    /**
     * Creates string - comma-separated attributes of test table, surrounded with braces
     * Is used as a part of sql statement
     *
     * @return attributes list of test table as part of sql statement
     */
    private String attributes(){
        StringBuilder attrs = new StringBuilder("(id");

        for (int vi = 1; vi <= valFieldsCnt; vi++)
            attrs.append(", val_").append(vi);

        attrs.append(')');

        return attrs.toString();
    }

    public void setRandomInsertArgs(PreparedStatement stmt, long id) throws SQLException{
        stmt.setLong(1, id);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int vi = 1; vi <= valFieldsCnt ; vi++) {
            // vi is value index (among all values), but we also have "id" which is primary key
            // so index in query is value index shifted by 1
            int qryIdx = vi + 1;

            long nextVal = rnd.nextLong();

            if (vi % 2 == 1)
                stmt.setLong(qryIdx, nextVal);
            else
                // FIXME: use pre-generated values
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
}
