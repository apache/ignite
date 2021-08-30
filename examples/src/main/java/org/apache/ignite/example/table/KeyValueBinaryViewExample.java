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
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;

/**
 * This example demonstrates the usage of the {@link KeyValueBinaryView} API.
 * <p>
 * To run the example, do the following:
 * <ol>
 *     <li>Import the examples project into you IDE.</li>
 *     <li>
 *         (optional) Run one or more standalone nodes using the CLI tool:<br>
 *         {@code ignite node start --config=$IGNITE_HOME/examples/config/ignite-config.json node-1}<br>
 *         {@code ignite node start --config=$IGNITE_HOME/examples/config/ignite-config.json node-2}<br>
 *         {@code ...}<br>
*          {@code ignite node start --config=$IGNITE_HOME/examples/config/ignite-config.json node-n}<br>
 *     </li>
 *     <li>Run the example in the IDE.</li>
 * </ol>
 */
public class KeyValueBinaryViewExample {
    public static void main(String[] args) throws Exception {
        Ignite ignite = IgnitionManager.start(
            "node-0",
            Files.readString(Path.of("config", "ignite-config.json").toAbsolutePath()),
            Path.of("work")
        );

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

        KeyValueBinaryView kvView = accounts.kvView();

        //---------------------------------------------------------------------------------
        //
        // Tuple API: insert operation.
        //
        //---------------------------------------------------------------------------------

        Tuple key = Tuple.create()
            .set("accountNumber", 123456);

        Tuple value = Tuple.create()
            .set("firstName", "Val")
            .set("lastName", "Kulichenko")
            .set("balance", 100.00d);

        kvView.put(key, value);

        //---------------------------------------------------------------------------------
        //
        // Tuple API: get operation.
        //
        //---------------------------------------------------------------------------------

        value = accounts.get(key);

        System.out.println(
            "Retrieved using Key-Value API\n" +
            "    Account Number: " + key.intValue("accountNumber") + '\n' +
            "    Owner: " + value.stringValue("firstName") + " " + value.stringValue("lastName") + '\n' +
            "    Balance: $" + value.doubleValue("balance"));
    }
}
