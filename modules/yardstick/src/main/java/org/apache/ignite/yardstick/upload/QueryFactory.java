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

        for (int i = 1; i <= valFieldsCnt; i++) {
            create.append(", val_").append(i);

            if (i % 2 == 1)
                create.append(" VARCHAR(255)");
            else
                create.append(" LONG");

        }

        create.append(");");

        return create.toString();
    }

    private String newInsertQuery() {
        StringBuilder insert = new StringBuilder("INSERT INTO test_upload VALUES (?");
        for (int i = 1; i <= valFieldsCnt; i++)
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

    public void setRandomInsertArgs(PreparedStatement stmt, long id) throws SQLException{
        stmt.setLong(1, id);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 1; i <= valFieldsCnt ; i++) {
            // "id" field has index 1, i is value index,
            // so parameter indexes of values should be shifted by 1
            int qryIdx = i + 1;

            long nextVal = rnd.nextLong();

            if (i % 2 == 1)
                stmt.setLong(qryIdx, nextVal);
            else
                // FIXME: use pre-generated values
                stmt.setString(qryIdx, String.valueOf(nextVal));
        }
    }
}
