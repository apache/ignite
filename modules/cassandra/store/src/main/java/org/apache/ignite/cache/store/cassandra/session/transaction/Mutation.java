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

package org.apache.ignite.cache.store.cassandra.session.transaction;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;

/**
 * Provides information about particular mutation operation performed withing transaction.
 */
public interface Mutation {
    /**
     * Cassandra table to use for an operation.
     *
     * @return Table name.
     */
    public String getTable();

    /**
     * Indicates if Cassandra tables existence is required for this operation.
     *
     * @return {@code true} true if table existence required.
     */
    public boolean tableExistenceRequired();

    /**
     *  Returns Ignite cache key/value persistence settings.
     *
     * @return persistence settings.
     */
    public KeyValuePersistenceSettings getPersistenceSettings();

    /**
     * Returns unbind CLQ statement for to be executed.
     *
     * @return Unbind CQL statement.
     */
    public String getStatement();

    /**
     * Binds prepared statement to current Cassandra session.
     *
     * @param statement Statement.
     * @return Bounded statement.
     */
    public BoundStatement bindStatement(PreparedStatement statement);
}
