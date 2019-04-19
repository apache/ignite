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
