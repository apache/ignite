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

package org.apache.ignite.internal.runner.app.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests thin client connecting to a real server node.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItThinClientConnectionTest extends ItAbstractThinClientTest {
    /**
     * Check that thin client can connect to any server node and work with table API.
     */
    @Test
    void testThinClientConnectsToServerNodesAndExecutesBasicTableOperations() throws Exception {
        for (var addr : getNodeAddresses()) {
            try (var client = IgniteClient.builder().addresses(addr).build()) {
                List<Table> tables = client.tables().tables();
                assertEquals(1, tables.size());

                Table table = tables.get(0);
                assertEquals(String.format("%s.%s", SCHEMA_NAME, TABLE_NAME), table.name());

                var tuple = Tuple.create().set(COLUMN_KEY, 1).set(COLUMN_VAL, "Hello");
                var keyTuple = Tuple.create().set(COLUMN_KEY, 1);

                RecordView<Tuple> recView = table.recordView();

                recView.upsert(null, tuple);
                assertEquals("Hello", recView.get(null, keyTuple).stringValue(COLUMN_VAL));

                var kvView = table.keyValueView();
                assertEquals("Hello", kvView.get(null, keyTuple).stringValue(COLUMN_VAL));

                var pojoView = table.recordView(TestPojo.class);
                assertEquals("Hello", pojoView.get(null, new TestPojo(1)).val);

                assertTrue(recView.delete(null, keyTuple));
            }
        }
    }

    private static class TestPojo {
        public TestPojo() {
            //No-op.
        }

        public TestPojo(int key) {
            this.key = key;
        }

        public int key;

        public String val;
    }
}
