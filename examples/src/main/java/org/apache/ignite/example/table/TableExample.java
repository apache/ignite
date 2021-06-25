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

package org.apache.ignite.example.table;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.app.IgnitionManager;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;

public class TableExample {
    public static void main(String[] args) throws Exception {
        String config = Files.readString(Path.of(ClassLoader.getSystemResource("ignite-config.json").toURI()));

        try (Ignite ignite = IgnitionManager.start("node0", config)) {

            //---------------------------------------------------------------------------------
            //
            // Creating a table. The API call below is the equivalent of the following DDL:
            //
            //     CREATE TABLE accounts (
            //         accountNumber INT PRIMARY KEY,
            //         firstName     VARCHAR,
            //         lastName      VARCHAR,
            //         balance       DOUBLE
            //     )
            //
            //---------------------------------------------------------------------------------

            Table accounts = ignite.tables().createTable("PUBLIC.accounts", tbl -> tbl
                .changeName("PUBLIC.accounts")
                .changeColumns(cols -> cols
                    .create("0", c -> c.changeName("accountNumber").changeType(t -> t.changeType("int32")).changeNullable(false))
                    .create("1", c -> c.changeName("firstName").changeType(t -> t.changeType("string")).changeNullable(true))
                    .create("2", c -> c.changeName("lastName").changeType(t -> t.changeType("string")).changeNullable(true))
                    .create("3", c -> c.changeName("balance").changeType(t -> t.changeType("double")).changeNullable(true))
                )
                .changeIndices(idxs -> idxs
                    .create("PK", idx -> idx
                        .changeName("PK")
                        .changeType("PK")
                        .changeColumns(cols -> cols.create("0", c -> c.changeName("accountNumber").changeAsc(true)))
                    )
                )
            );

            //---------------------------------------------------------------------------------
            //
            // Tuple API: insert operation.
            //
            //---------------------------------------------------------------------------------

            Tuple newAccountTuple = accounts.tupleBuilder()
                .set("accountNumber", 123456)
                .set("firstName", "Val")
                .set("lastName", "Kulichenko")
                .set("balance", 100.00d)
                .build();

            accounts.insert(newAccountTuple);

            //---------------------------------------------------------------------------------
            //
            // Tuple API: get operation.
            //
            //---------------------------------------------------------------------------------

            Tuple accountNumberTuple = accounts.tupleBuilder().set("accountNumber", 123456).build();

            Tuple accountTuple = accounts.get(accountNumberTuple);

            System.out.println(
                "Retrieved using Tuple API\n" +
                    "  Account Number: " + accountTuple.intValue("accountNumber") + '\n' +
                    "  Owner: " + accountTuple.stringValue("firstName") + " " + accountTuple.stringValue("lastName") + '\n' +
                    "  Balance: $" + accountTuple.doubleValue("balance"));
        }
    }
}
