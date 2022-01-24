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
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.table.Table;

/**
 * Interface that provides methods for managing tables.
 */
public interface IgniteTables {
    /**
     * Creates a new table with the given {@code name}. If a table with the same name already exists, an exception will be thrown.
     *
     * @param name Canonical name of the table ([schemaName].[tableName]) with SQL-parser style quotation, e.g.
     *             "public.tbl0" - the table "PUBLIC.TBL0" will be created,
     *             "PUBLIC.\"Tbl0\"" - "PUBLIC.Tbl0", "\"MySchema\".\"Tbl0\"" - "MySchema.Tbl0", etc.
     * @param tableInitChange Table changer.
     * @return Newly created table.
     * @throws TableAlreadyExistsException If table with given name already exists.
     * @throws IgniteException             If an unspecified platform exception has happened internally. Is thrown when:
     *                                     <ul>
     *                                         <li>the node is stopping.</li>
     *                                     </ul>
     */
    Table createTable(String name, Consumer<TableChange> tableInitChange);

    /**
     * Creates a new table with the given {@code name} asynchronously. If a table with the same name already exists, a future will be
     * completed with the {@link TableAlreadyExistsException}.
     *
     * @param name Canonical name of the table ([schemaName].[tableName]) with SQL-parser style quotation, e.g.
     *             "public.tbl0" - the table "PUBLIC.TBL0" will be created,
     *             "PUBLIC.\"Tbl0\"" - "PUBLIC.Tbl0", "\"MySchema\".\"Tbl0\"" - "MySchema.Tbl0", etc.
     * @param tableInitChange Table changer.
     * @return Future representing pending completion of the operation.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     * @see TableAlreadyExistsException
     */
    CompletableFuture<Table> createTableAsync(String name, Consumer<TableChange> tableInitChange);

    /**
     * Alters a cluster table. If the appropriate table does not exist, an exception will be thrown.
     *
     * @param name Canonical name of the table ([schemaName].[tableName]) with SQL-parser style quotation, e.g.
     *             "public.tbl0" - the table "PUBLIC.TBL0" will be changed,
     *             "PUBLIC.\"Tbl0\"" - "PUBLIC.Tbl0", "\"MySchema\".\"Tbl0\"" - "MySchema.Tbl0", etc.
     * @param tableChange Table changer.
     * @throws TableNotFoundException If a table with the {@code name} does not exist.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     */
    void alterTable(String name, Consumer<TableChange> tableChange);

    /**
     * Alters a cluster table. If the appropriate table does not exist, a future will
     * complete with the {@link TableNotFoundException}.
     *
     * @param name Canonical name of the table ([schemaName].[tableName]) with SQL-parser style quotation, e.g.
     *             "public.tbl0" - the table "PUBLIC.TBL0" will be changed,
     *             "PUBLIC.\"Tbl0\"" - "PUBLIC.Tbl0", "\"MySchema\".\"Tbl0\"" - "MySchema.Tbl0", etc.
     * @param tableChange Table changer.
     * @return Future representing pending completion of the operation.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     * @see TableNotFoundException
     */
    CompletableFuture<Void> alterTableAsync(String name, Consumer<TableChange> tableChange);

    /**
     * Drops a table with the name specified. If the appropriate table does not exist, an exception will be thrown.
     *
     * @param name Canonical name of the table ([schemaName].[tableName]) with SQL-parser style quotation, e.g.
     *             "public.tbl0" - the table "PUBLIC.TBL0" will be dropped,
     *             "PUBLIC.\"Tbl0\"" - "PUBLIC.Tbl0", "\"MySchema\".\"Tbl0\"" - "MySchema.Tbl0", etc.
     * @throws TableNotFoundException If a table with the {@code name} does not exist.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     */
    void dropTable(String name);

    /**
     * Drops a table with the name specified. If the appropriate table does not exist, a future will
     * complete with the {@link TableNotFoundException}.
     *
     * @param name Canonical name of the table ([schemaName].[tableName]) with SQL-parser style quotation, e.g.
     *             "public.tbl0" - the table "PUBLIC.TBL0" will be dropped,
     *             "PUBLIC.\"Tbl0\"" - "PUBLIC.Tbl0", "\"MySchema\".\"Tbl0\"" - "MySchema.Tbl0", etc.
     * @return Future representing pending completion of the operation.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     * @see TableNotFoundException
     */
    CompletableFuture<Void> dropTableAsync(String name);

    /**
     * Gets a list of all started tables.
     *
     * @return List of tables.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     */
    List<Table> tables();

    /**
     * Gets a list of all started tables.
     *
     * @return Future representing pending completion of the operation.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     */
    CompletableFuture<List<Table>> tablesAsync();

    /**
     * Gets a table by name, if it was created before.
     *
     * @param name Canonical name of the table ([schemaName].[tableName]) with SQL-parser style quotation, e.g.
     *             "public.tbl0" - the table "PUBLIC.TBL0" will be looked up,
     *             "PUBLIC.\"Tbl0\"" - "PUBLIC.Tbl0", "\"MySchema\".\"Tbl0\"" - "MySchema.Tbl0", etc.
     * @return Tables with corresponding name or {@code null} if table isn't created.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     */
    Table table(String name);

    /**
     * Gets a table by name, if it was created before.
     *
     * @param name Canonical name of the table ([schemaName].[tableName]) with SQL-parser style quotation, e.g.
     *             "public.tbl0" - the table "PUBLIC.TBL0" will be looked up,
     *             "PUBLIC.\"Tbl0\"" - "PUBLIC.Tbl0", "\"MySchema\".\"Tbl0\"" - "MySchema.Tbl0", etc.
     * @return Future representing pending completion of the operation.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     */
    CompletableFuture<Table> tableAsync(String name);
}
