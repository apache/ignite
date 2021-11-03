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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;

/**
 * This example demonstrates the usage of the {@link KeyValueView} API.
 *
 * <p>To run the example, do the following:
 * <ol>
 *     <li>Import the examples project into you IDE.</li>
 *     <li>
 *         Start a server node using the CLI tool:<br>
 *         {@code ignite node start --config=$IGNITE_HOME/examples/config/ignite-config.json my-first-node}
 *     </li>
 *     <li>Run the example in the IDE.</li>
 * </ol>
 */
public class KeyValueViewExample {
    public static void main(String[] args) throws Exception {
        //--------------------------------------------------------------------------------------
        //
        // Starting a server node.
        //
        // NOTE: An embedded server node is only needed to invoke the 'createTable' API.
        //       In the future releases, this API will be available on the client,
        //       eliminating the need to start an embedded server node in this example.
        //
        //--------------------------------------------------------------------------------------

        System.out.println("Starting a server node... Logging to file: example-node.log");

        System.setProperty("java.util.logging.config.file", "config/java.util.logging.properties");

        try (Ignite server = IgnitionManager.start(
                "example-node",
                Files.readString(Path.of("config", "ignite-config.json")),
                Path.of("work")
        )) {
            //--------------------------------------------------------------------------------------
            //
            // Creating 'accounts' table. The API call below is the equivalent of the following DDL:
            //
            //     CREATE TABLE accounts (
            //         accountNumber INT PRIMARY KEY,
            //         firstName     VARCHAR,
            //         lastName      VARCHAR,
            //         balance       DOUBLE
            //     )
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nCreating 'accounts' table...");

            TableDefinition accountsTableDef = SchemaBuilders.tableBuilder("PUBLIC", "accounts")
                    .columns(
                            SchemaBuilders.column("accountNumber", ColumnType.INT32).asNonNull().build(),
                            SchemaBuilders.column("firstName", ColumnType.string()).asNullable().build(),
                            SchemaBuilders.column("lastName", ColumnType.string()).asNullable().build(),
                            SchemaBuilders.column("balance", ColumnType.DOUBLE).asNullable().build()
                    )
                    .withPrimaryKey("accountNumber")
                    .build();

            server.tables().createTable(accountsTableDef.canonicalName(), tableChange ->
                    SchemaConfigurationConverter.convert(accountsTableDef, tableChange)
                            .changeReplicas(1)
                            .changePartitions(10)
            );

            //--------------------------------------------------------------------------------------
            //
            // Creating a client to connect to the cluster.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nConnecting to server...");

            try (IgniteClient client = IgniteClient.builder()
                    .addresses("127.0.0.1:10800")
                    .build()
            ) {
                //--------------------------------------------------------------------------------------
                //
                // Creating a key-value view for the 'accounts' table.
                //
                //--------------------------------------------------------------------------------------

                KeyValueView<Tuple, Tuple> kvView = client.tables().table("PUBLIC.accounts").keyValueView();

                //--------------------------------------------------------------------------------------
                //
                // Performing the 'put' operation.
                //
                //--------------------------------------------------------------------------------------

                System.out.println("\nInserting a key-value pair into the 'accounts' table...");

                Tuple key = Tuple.create()
                        .set("accountNumber", 123456);

                Tuple value = Tuple.create()
                        .set("firstName", "Val")
                        .set("lastName", "Kulichenko")
                        .set("balance", 100.00d);

                kvView.put(key, value);

                //--------------------------------------------------------------------------------------
                //
                // Performing the 'get' operation.
                //
                //--------------------------------------------------------------------------------------

                System.out.println("\nRetrieving a value using KeyValueView API...");

                value = kvView.get(key);

                System.out.println(
                        "\nRetrieved value:\n"
                                + "    Account Number: " + key.intValue("accountNumber") + '\n'
                                + "    Owner: " + value.stringValue("firstName") + " " + value.stringValue("lastName") + '\n'
                                + "    Balance: $" + value.doubleValue("balance"));
            }

            System.out.println("\nDropping the table and stopping the server...");

            server.tables().dropTable(accountsTableDef.canonicalName());
        }
    }
}
