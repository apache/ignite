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
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;

/**
 * This example demonstrates data rebalance on new nodes.
 *
 * - Start 3 nodes (A, B, C)
 * - Put some data
 * - Add 2 new nodes (D, E)
 * - Set new baseline to (A, D, E)
 * - Stop (B, C)
 * - Check that data is still available on the new configuration
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
        var nodes = new ArrayList<Ignite>();

        for (int i = 0; i < 3; i++) {
            nodes.add(IgnitionManager.start("node-" + i,
                Files.readString(Path.of("config", "rebalance", "ignite-config-" + i + ".json")),
                Path.of("work" + i)));
        }

        var node0 = nodes.get(0);

        Table testTable = node0.tables().createTable("PUBLIC.rebalance", tbl ->
            SchemaConfigurationConverter.convert(
                SchemaBuilders.tableBuilder("PUBLIC", "rebalance")
                    .columns(
                        SchemaBuilders.column("key", ColumnType.INT32).asNonNull().build(),
                        SchemaBuilders.column("value", ColumnType.string()).asNullable().build()
                    )
                    .withPrimaryKey("key")
                    .build(), tbl)
                .changeReplicas(5)
                .changePartitions(1)
        );

        KeyValueView<Tuple, Tuple> kvView = testTable.keyValueView();
        Tuple key = Tuple.create()
            .set("key", 1);

        Tuple value = Tuple.create()
            .set("value", "test");

        kvView.put(key, value);

        String oldValue = testTable.recordView().get(key).value("value");
        System.out.println("Value in the start cluster configuration " + oldValue);

        for (int i = 3; i < 5; i++) {
            nodes.add(IgnitionManager.start("node-" + i,
                Files.readString(Path.of("config", "rebalance", "ignite-config-" + i + ".json")),
                Path.of("work" + i)));
        }

        node0.setBaseline(Set.of("node-0", "node-3", "node-4"));

        IgnitionManager.stop(nodes.get(1).name());
        IgnitionManager.stop(nodes.get(2).name());

        var valueOnNewNode = nodes.get(4).tables().table("PUBLIC.rebalance").recordView().get(key).value("value");

        System.out.println("Value in the new cluster configuration " + valueOnNewNode);

        IgnitionManager.stop(nodes.get(0).name());
        IgnitionManager.stop(nodes.get(3).name());
        IgnitionManager.stop(nodes.get(4).name());
    }
}
