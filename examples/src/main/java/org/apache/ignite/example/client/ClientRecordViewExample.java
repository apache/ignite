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

package org.apache.ignite.example.client;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;

/**
 * This example demonstrates the usage of the {@link Table} API of Java client.
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
public class ClientRecordViewExample {
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = IgnitionManager.start(
                "node-0",
                Files.readString(Path.of("config", "ignite-config.json")),
                Path.of("work")
        )) {

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

             ignite.tables().createTable("PUBLIC.accounts", tbl ->
                SchemaConfigurationConverter.convert(
                    SchemaBuilders.tableBuilder("PUBLIC", "accounts")
                        .columns(
                            SchemaBuilders.column("accountNumber", ColumnType.INT32).asNonNull().build(),
                            SchemaBuilders.column("firstName", ColumnType.string()).asNullable().build(),
                            SchemaBuilders.column("lastName", ColumnType.string()).asNullable().build(),
                            SchemaBuilders.column("balance", ColumnType.DOUBLE).asNullable().build()
                        )
                        .withPrimaryKey("accountNumber")
                        .build(), tbl)
                    .changeReplicas(1)
                    .changePartitions(10)
            );

            try (IgniteClient client = IgniteClient.builder()
                    .addresses("127.0.0.1:10800")
                    .build()
            ) {
                RecordView<Tuple> accounts = client.tables().table("PUBLIC.accounts").recordView();

                //---------------------------------------------------------------------------------
                //
                // Tuple API: insert operation.
                //
                //---------------------------------------------------------------------------------

                Tuple newAccountTuple = Tuple.create()
                    .set("accountNumber", 123456)
                    .set("firstName", "Val")
                    .set("lastName", "Kulichenko")
                    .set("balance", 100.00d);

                accounts.insert(newAccountTuple);

                //---------------------------------------------------------------------------------
                //
                // Tuple API: get operation.
                //
                //---------------------------------------------------------------------------------

                Tuple accountNumberTuple = Tuple.create().set("accountNumber", 123456);

                Tuple accountTuple = accounts.get(accountNumberTuple);

                System.out.println(
                    "Retrieved using Tuple API\n" +
                        "    Account Number: " + accountTuple.intValue("accountNumber") + '\n' +
                        "    Owner: " + accountTuple.stringValue("firstName") + " " + accountTuple.stringValue("lastName") + '\n' +
                        "    Balance: $" + accountTuple.doubleValue("balance"));
            }
        }
    }
}
