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

package org.apache.ignite.table.manager;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.table.Table;

/**
 * Interface that provides methods for managing tables.
 */
public interface IgniteTables {
    /**
     * Creates a new table with the given {@code name}.
     * If a table with the same name already exists, an exception will be thrown.
     *
     * @param name Table name.
     * @param tableInitChange Table changer.
     * @return Newly created table.
     * @throws TableAlreadyExistsException If table with given name already exists.
     */
    Table createTable(String name, Consumer<TableChange> tableInitChange);

    /**
     * Creates a new table with the given {@code name} asynchronously.
     * If a table with the same name already exists, a future will be completed with exception.
     *
     * @param name Table name.
     * @param tableInitChange Table changer.
     * @return Future representing pending completion of the operation.
     * @see TableAlreadyExistsException
     */
    CompletableFuture<Table> createTableAsync(String name, Consumer<TableChange> tableInitChange);

    /**
     * Creates a new table with the given {@code name} or returns an existing one with the same {@code name}.
     *
     * Note: the configuration of the existed table will NOT be validated against the given {@code tableInitChange}.
     *
     * @param name Table name.
     * @param tableInitChange Table changer.
     * @return Existing or newly created table.
     */
    Table createTableIfNotExists(String name, Consumer<TableChange> tableInitChange);

    /**
     * Creates a new table with the given {@code name} or returns an existing one with the same {@code name}.
     *
     * Note: the configuration of the existed table will NOT be validated against the given {@code tableInitChange}.
     *
     * @param name Table name.
     * @param tableInitChange Table changer.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Table> createTableIfNotExistsAsync(String name, Consumer<TableChange> tableInitChange);

    /**
     * Alter a cluster table.
     *
     * @param name Table name.
     * @param tableChange Table changer.
     */
    void alterTable(String name, Consumer<TableChange> tableChange);

    /**
     * Alter a cluster table.
     *
     * @param name Table name.
     * @param tableChange Table changer.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Void> alterTableAsync(String name, Consumer<TableChange> tableChange);

    /**
     * Drops a table with the name specified.
     * If a table with the specified name does not exist in the cluster, the operation has no effect.
     *
     * @param name Table name.
     */
    void dropTable(String name);

    /**
     * Drops a table with the name specified.
     * If a table with the specified name does not exist in the cluster, the operation has no effect.
     *
     * @param name Table name.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Void> dropTableAsync(String name);

    /**
     * Gets a list of all started tables.
     *
     * @return List of tables.
     */
    List<Table> tables();

    /**
     * Gets a list of all started tables.
     *
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<List<Table>> tablesAsync();

    /**
     * Gets a table by name, if it was created before.
     *
     * @param name Name of the table.
     * @return Tables with corresponding name or {@code null} if table isn't created.
     */
    Table table(String name);

    /**
     * Gets a table by name, if it was created before.
     *
     * @param name Name of the table.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Table> tableAsync(String name);
}
