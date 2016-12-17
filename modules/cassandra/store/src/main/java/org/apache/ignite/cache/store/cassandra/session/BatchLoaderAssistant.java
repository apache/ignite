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

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;

/**
 * Provides information for loadCache operation of {@link org.apache.ignite.cache.store.cassandra.CassandraCacheStore}.
 */
public interface BatchLoaderAssistant {
    /**
     * Returns name of the batch load operation.
     *
     * @return operation name.
     */
    public String operationName();

    /**
     * Returns CQL statement to use in batch load operation.
     *
     * @return CQL statement for batch load operation.
     */
    public Statement getStatement();

    /**
     * Processes each row returned by batch load operation.
     *
     * @param row row selected from Cassandra table.
     */
    public void process(Row row);
}
