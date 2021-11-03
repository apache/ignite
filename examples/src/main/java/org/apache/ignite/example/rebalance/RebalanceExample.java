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
import java.util.Set;
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
 * This example demonstrates the data rebalance process.
 *
 * <p>The example emulates the basic scenario when one starts a three-node topology,
 * inserts some data, and then scales out by adding two more nodes. After the topology is changed, the data is rebalanced and verified for
 * correctness.
 *
 * <p>To run the example, do the following:
 * <ol>
 *     <li>Import the examples project into you IDE.</li>
 *     <li>
 *         Start <b>two</b> nodes using the CLI tool:<br>
 *         {@code ignite node start --config=$IGNITE_HOME/examples/config/ignite-config.json my-first-node}<br>
 *         {@code ignite node start --config=$IGNITE_HOME/examples/config/ignite-config.json my-second-node}
 *     </li>
 *     <li>Run the example in the IDE.</li>
 *     <li>
 *         When requested, start another two nodes using the CLI tool:
 *         {@code ignite node start --config=$IGNITE_HOME/examples/config/ignite-config.json my-first-additional-node}<br>
 *         {@code ignite node start --config=$IGNITE_HOME/examples/config/ignite-config.json my-second-additional-node}
 *     </li>
 *     <li>Press {@code Enter} to resume the example.</li>
 * </ol>
 */
public class RebalanceExample {
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
            
            server.tables().createTable(tableDef.canonicalName(), tableChange ->
                    SchemaConfigurationConverter.convert(tableDef, tableChange)
                            .changeReplicas(5)
                            .changePartitions(1)
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
                KeyValueView<Tuple, Tuple> kvView = client.tables().table("PUBLIC.rebalance").keyValueView();
                
                //--------------------------------------------------------------------------------------
                //
                // Inserting several key-value pairs into the table.
                //
                //--------------------------------------------------------------------------------------
                
                System.out.println("\nInserting key-value pairs...");
                
                for (int i = 0; i < 10; i++) {
                    Tuple key = Tuple.create().set("key", i);
                    Tuple value = Tuple.create().set("value", "test_" + i);
                    
                    kvView.put(key, value);
                }
                
                //--------------------------------------------------------------------------------------
                //
                // Retrieving the newly inserted data.
                //
                //--------------------------------------------------------------------------------------
                
                System.out.println("\nRetrieved key-value pairs:");
                
                for (int i = 0; i < 10; i++) {
                    Tuple key = Tuple.create().set("key", i);
                    Tuple value = kvView.get(key);
                    
                    System.out.println("    " + i + " -> " + value.stringValue("value"));
                }
                
                //--------------------------------------------------------------------------------------
                //
                // Scaling out by adding two more nodes into the topology.
                //
                //--------------------------------------------------------------------------------------
                
                System.out.println("\n"
                        + "Run the following commands using the CLI tool to start two more nodes, and then press 'Enter' to continue...\n"
                        + "    ignite node start --config=examples/config/ignite-config.json my-first-additional-node\n"
                        + "    ignite node start --config=examples/config/ignite-config.json my-second-additional-node");
                
                System.in.read();
                
                //--------------------------------------------------------------------------------------
                //
                // Updating baseline to initiate the data rebalancing process.
                //
                // New topology includes the following five nodes:
                //     1. 'my-first-node' -- the first node started prior to running the example
                //     2. 'my-second-node' -- the second node started prior to running the example
                //     3. 'example-node' -- node that is embedded into the example
                //     4. 'additional-node-1' -- the first node added to the topology
                //     5. 'additional-node-2' -- the second node added to the topology
                //
                // NOTE: In the future releases, this API will be provided by the clients as well.
                //       In addition, the process will be automated where applicable to eliminate
                //       the need for this manual step.
                //
                //--------------------------------------------------------------------------------------
                
                System.out.println("\nUpdating the baseline and rebalancing the data...");
                
                server.setBaseline(Set.of(
                        "my-first-node",
                        "my-second-node",
                        "example-node",
                        "my-first-additional-node",
                        "my-second-additional-node"
                ));
                
                //--------------------------------------------------------------------------------------
                //
                // Retrieving data again to validate correctness.
                //
                //--------------------------------------------------------------------------------------
                
                System.out.println("\nKey-value pairs retrieved after the topology change:");
                
                for (int i = 0; i < 10; i++) {
                    Tuple key = Tuple.create().set("key", i);
                    Tuple value = kvView.get(key);
                    
                    System.out.println("    " + i + " -> " + value.stringValue("value"));
                }
            }
            
            System.out.println("\nDropping the table and stopping the server...");
            
            server.tables().dropTable(tableDef.canonicalName());
        }
    }
}
