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

package org.apache.ignite.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests client tables.
 */
public class ClientTablesTest extends AbstractClientTest {
    @Test
    public void testTablesWhenTablesExist() {
        server.tables().createTable(DEFAULT_TABLE, null);
        server.tables().createTable("t", null);

        var tables = client.tables().tables();
        assertEquals(2, tables.size());

        tables.sort(Comparator.comparing(Table::tableName));
        assertEquals(DEFAULT_TABLE, tables.get(0).tableName());
        assertEquals("t", tables.get(1).tableName());
    }

    @Test
    public void testTablesWhenNoTablesExist() {
        var tables = client.tables().tables();

        assertEquals(0, tables.size());
    }

    @Test
    @Disabled("IGNITE-15179")
    public void testCreateTable() {
        var clientTable = client.tables().createTable("t1", t -> t.changeReplicas(2));
        assertEquals("t1", clientTable.tableName());

        var serverTables = server.tables().tables();
        assertEquals(1, serverTables.size());

        var serverTable = serverTables.get(0);
        assertEquals("t1", serverTable.tableName());
        assertEquals(((TableImpl) serverTable).tableId(), ((ClientTable) clientTable).tableId());
    }

    @Test
    @Disabled("IGNITE-15179")
    public void testCreateTableWhenExists() {
        Consumer<TableChange> consumer = t -> t.changeReplicas(2);
        client.tables().createTable(DEFAULT_TABLE, consumer);

        var ex = assertThrows(CompletionException.class,
                () -> client.tables().createTable(DEFAULT_TABLE, consumer));

        assertTrue(ex.getMessage().endsWith(FakeIgniteTables.TABLE_EXISTS));
        assertEquals(IgniteClientException.class, ex.getCause().getClass());

        var serverTables = server.tables().tables();
        assertEquals(1, serverTables.size());
    }

    @Test
    public void testDropTable() {
        server.tables().createTable("t", t -> t.changeReplicas(0));

        client.tables().dropTable("t");

        assertEquals(0, server.tables().tables().size());
    }

    @Test
    public void testDropTableInvalidName() {
        client.tables().dropTable("foo");
    }
}
