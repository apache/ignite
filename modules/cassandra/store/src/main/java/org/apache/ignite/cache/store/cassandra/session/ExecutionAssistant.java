/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache.store.cassandra.session;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;

/**
 * Provides information for single operations (load, delete, write) of Ignite cache
 * backed by {@link org.apache.ignite.cache.store.cassandra.CassandraCacheStore}.
 *
 * @param <R> type of the result returned from operation.
 */
public interface ExecutionAssistant<R> {
    /**
     * Indicates if Cassandra table existence is required for an operation.
     *
     * @return true if table existence required.
     */
    public boolean tableExistenceRequired();

    /**
     * Cassandra table to use for an operation.
     *
     * @return Table name.
     */
    public String getTable();

    /**
     * Returns CQL statement to be used for an operation.
     *
     * @return CQL statement.
     */
    public String getStatement();

    /**
     * Binds prepared statement.
     *
     * @param statement prepared statement.
     *
     * @return bound statement.
     */
    public BoundStatement bindStatement(PreparedStatement statement);

    /**
     * Persistence settings to use for an operation.
     *
     * @return persistence settings.
     */
    public KeyValuePersistenceSettings getPersistenceSettings();

    /**
     * Returns operation name.
     *
     * @return operation name.
     */
    public String operationName();

    /**
     * Processes Cassandra database table row returned by specified CQL statement.
     *
     * @param row Cassandra database table row.
     *
     * @return result of the operation.
     */
    public R process(Row row);
}
