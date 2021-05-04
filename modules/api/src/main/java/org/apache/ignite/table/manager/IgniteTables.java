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

/**
 * Interface for manage tables.
 */
package org.apache.ignite.table.manager;

import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.table.Table;

/**
 * Interface that provides methods for managing tables.
 */
public interface IgniteTables {
    /**
     * Creates a cluster table.
     * The table changes if already exists.
     *
     * @param name Table name.
     * @param tableInitChange Table changer.
     * @return Table.
     */
    Table createTable(String name, Consumer<TableChange> tableInitChange);

    /**
     * Drops a table with the name specified.
     *
     * @param name Table name.
     */
    void dropTable(String name);

    /**
     * Gets a list of all started tables.
     *
     * @return List of tables.
     */
    List<Table> tables();

    /**
     * Gets a table by name, if it was created before.
     *
     * @param name Name of the table.
     * @return Tables with corresponding name or {@code null} if table isn't created.
     */
    Table table(String name);
}
