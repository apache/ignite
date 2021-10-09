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

package org.apache.ignite.example.rebalance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;

/**
 * This example demonstrates the data rebalance process.
 * The following sequence of events is emulated:
 *
 * <ul>
 * <li>Start 3 nodes (A, B and C)</li>
 * <li>Insert some data</li>
 * <li>Add 2 more nodes (D and E)</li>
 * <li>Set new baseline to (A, D, E)</li>
 * <li>Stop nodes B and C</li>
 * <li>Check that data is still available on the new configuration</li>
 * </ul>
 *
 * <p>
 * To run the example, do the following:
 * <ol>
 *     <li>Import the examples project into you IDE.</li>
 *     <li>Run the example in the IDE.</li>
 * </ol>
 */
public class RebalanceExample {
    public static void main(String[] args) throws Exception {
        List<Ignite> nodes = new ArrayList<>();

        try {
            System.out.println("Starting server nodes... Logging to file: ignite.log");

            System.setProperty("java.util.logging.config.file", "config/java.util.logging.properties");

            for (int i = 0; i < 3; i++) {
                nodes.add(IgnitionManager.start("node-" + i,
                    Files.readString(Path.of("config", "rebalance", "ignite-config-" + i + ".json")),
                    Path.of("work" + i)));
            }

            Ignite node0 = nodes.get(0);

            //--------------------------------------------------------------------------------------
            //
            // Creating a table. The API call below is the equivalent of the following DDL:
            //
            //     CREATE TABLE rebalance (
            //         key   INT PRIMARY KEY,
            //         value VARCHAR
            //     )
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nCreating a table...");

            TableDefinition tableDef = SchemaBuilders.tableBuilder("PUBLIC", "rebalance")
                .columns(
                    SchemaBuilders.column("key", ColumnType.INT32).asNonNull().build(),
                    SchemaBuilders.column("value", ColumnType.string()).asNullable().build()
                )
                .withPrimaryKey("key")
                .build();

            Table testTable = node0.tables().createTable("PUBLIC.rebalance", tableChange ->
                SchemaConfigurationConverter.convert(tableDef, tableChange)
                    .changeReplicas(5)
                    .changePartitions(1)
            );

            //--------------------------------------------------------------------------------------
            //
            // Inserting a key-value pair into the table.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nInserting a key-value pair...");

            KeyValueView<Tuple, Tuple> kvView = testTable.keyValueView();

            Tuple key = Tuple.create().set("key", 1);
            Tuple value = Tuple.create().set("value", "test");

            kvView.put(key, value);

            System.out.println("\nCurrent value: " + kvView.get(key).value("value"));

            //--------------------------------------------------------------------------------------
            //
            // Changing the topology and updating the baseline.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nChanging the topology...");

            for (int i = 3; i < 5; i++) {
                nodes.add(IgnitionManager.start("node-" + i,
                    Files.readString(Path.of("config", "rebalance", "ignite-config-" + i + ".json")),
                    Path.of("work" + i)));
            }

            node0.setBaseline(Set.of("node-0", "node-3", "node-4"));

            IgnitionManager.stop(nodes.get(1).name());
            IgnitionManager.stop(nodes.get(2).name());

            //--------------------------------------------------------------------------------------
            //
            // Retrieving the value from one of the new nodes.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nRetrieving the value from one of the new nodes...");

            String valueOnNewNode = nodes.get(4).tables().table("PUBLIC.rebalance").keyValueView().get(key).value("value");

            System.out.println("\nRetrieved value: " + valueOnNewNode);

            System.out.println("\nDropping the table and stopping the cluster...");

            node0.tables().dropTable(tableDef.canonicalName());
        }
        finally {
            IgnitionManager.stop(nodes.get(0).name());
            IgnitionManager.stop(nodes.get(3).name());
            IgnitionManager.stop(nodes.get(4).name());
        }
    }
}
