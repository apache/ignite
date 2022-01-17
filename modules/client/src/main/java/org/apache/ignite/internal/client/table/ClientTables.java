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

package org.apache.ignite.internal.client.table;

import static org.apache.ignite.internal.client.ClientUtils.sync;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.manager.IgniteTables;

/**
 * Client tables API implementation.
 */
public class ClientTables implements IgniteTables {
    private final ReliableChannel ch;

    /**
     * Constructor.
     *
     * @param ch Channel.
     */
    public ClientTables(ReliableChannel ch) {
        this.ch = ch;
    }

    /** {@inheritDoc} */
    @Override
    public Table createTable(String name, Consumer<TableChange> tableInitChange) {
        return sync(createTableAsync(name, tableInitChange));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Table> createTableAsync(String name, Consumer<TableChange> tableInitChange) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(tableInitChange);

        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public void alterTable(String name, Consumer<TableChange> tableChange) {
        sync(alterTableAsync(name, tableChange));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> alterTableAsync(String name, Consumer<TableChange> tableChange) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(tableChange);

        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public void dropTable(String name) {
        sync(dropTableAsync(name));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> dropTableAsync(String name) {
        Objects.requireNonNull(name);

        return ch.requestAsync(ClientOp.TABLE_DROP, w -> w.out().packString(name));
    }

    /** {@inheritDoc} */
    @Override
    public List<Table> tables() {
        return sync(tablesAsync());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<Table>> tablesAsync() {
        return ch.serviceAsync(ClientOp.TABLES_GET, r -> {
            var in = r.in();
            var cnt = in.unpackMapHeader();
            var res = new ArrayList<Table>(cnt);

            for (int i = 0; i < cnt; i++) {
                res.add(new ClientTable(ch, in.unpackIgniteUuid(), in.unpackString()));
            }

            return res;
        });
    }

    /** {@inheritDoc} */
    @Override
    public Table table(String name) {
        return sync(tableAsync(name));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Table> tableAsync(String name) {
        Objects.requireNonNull(name);

        return ch.serviceAsync(ClientOp.TABLE_GET, w -> w.out().packString(name),
                r -> r.in().tryUnpackNil() ? null : new ClientTable(ch, r.in().unpackIgniteUuid(), name));
    }
}
