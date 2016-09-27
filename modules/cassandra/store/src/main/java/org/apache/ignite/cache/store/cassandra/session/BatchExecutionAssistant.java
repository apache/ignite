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

package org.apache.ignite.cache.store.cassandra.session;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;

/**
 * Provides information for batch operations (loadAll, deleteAll, writeAll) of Ignite cache
 * backed by {@link org.apache.ignite.cache.store.cassandra.CassandraCacheStore}.
 *
 * @param <R> type of the result returned from batch operation.
 * @param <V> type of the value used in batch operation.
 */
public interface BatchExecutionAssistant<R, V> {
    /**
     * Indicates if Cassandra tables existence is required for this batch operation.
     *
     * @return {@code true} true if table existence required.
     */
    public boolean tableExistenceRequired();

    /**
     * Cassandra table to use for an operation.
     *
     * @return Table name.
     */
    public String getTable();

    /**
     * Returns unbind CLQ statement for to be executed inside batch operation.
     *
     * @return Unbind CQL statement.
     */
    public String getStatement();

    /**
     * Binds prepared statement to current Cassandra session.
     *
     * @param statement Statement.
     * @param obj Parameters for statement binding.
     * @return Bounded statement.
     */
    public BoundStatement bindStatement(PreparedStatement statement, V obj);

    /**
     *  Returns Ignite cache key/value persistence settings.
     *
     * @return persistence settings.
     */
    public KeyValuePersistenceSettings getPersistenceSettings();

    /**
     * Display name for the batch operation.
     *
     * @return Operation display name.
     */
    public String operationName();

    /**
     * Processes particular row inside batch operation.
     *
     * @param row Row to process.
     * @param seqNum Sequential number of the row.
     */
    public void process(Row row, int seqNum);

    /**
     * Checks if row/object with specified sequential number is already processed.
     *
     * @param seqNum object sequential number
     * @return {@code true} if object is already processed
     */
    public boolean alreadyProcessed(int seqNum);

    /**
     * @return number of processed objects/rows.
     */
    public int processedCount();

    /**
     * @return batch operation result.
     */
    public R processedData();
}
