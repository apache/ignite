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

package org.apache.ignite.cache.store.cassandra.utils.session;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import org.apache.ignite.cache.store.cassandra.utils.persistence.KeyValuePersistenceSettings;

/**
 * Provides information for single operations (load, delete, write) of Ignite cache
 * backed by ${@link org.apache.ignite.cache.store.cassandra.CassandraCacheStore}
 * @param <R> Type of the result returned from operation
 */
public interface ExecutionAssistant<R> {
    public boolean tableExistenceRequired();

    public String getStatement();

    public BoundStatement bindStatement(PreparedStatement statement);

    public KeyValuePersistenceSettings getPersistenceSettings();

    public String operationName();

    public R process(Row row);
}
