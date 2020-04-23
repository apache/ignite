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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * Simple container for Cassandra session and its generation number.
 */
public class WrappedSession {
    /** Cassandra driver session. **/
    final Session ses;

    /** Cassandra session generation number. **/
    final long generation;

    /**
     * Constructor.
     *
     * @param ses Cassandra session.
     * @param generation Cassandra session generation number.
     */
    WrappedSession(Session ses, long generation) {
        this.ses = ses;
        this.generation = generation;
    }

    /**
     * Prepares the provided query string.
     *
     * @param query the CQL query string to prepare
     * @return the prepared statement corresponding to {@code query}.
     * @throws NoHostAvailableException if no host in the cluster can be
     *                                  contacted successfully to prepare this query.
     */
    WrappedPreparedStatement prepare(String query) {
        return new WrappedPreparedStatement(ses.prepare(query), generation);
    }

    /**
     * Executes the provided query.
     *
     * @param statement The CQL query to execute (that can be any {@link Statement}).
     *
     * @return The result of the query. That result will never be null but can
     */
    ResultSet execute(Statement statement) {
        return ses.execute(statement);
    }

    /**
     * Executes the provided query.
     *
     * @param query The CQL query to execute (that can be any {@link Statement}).
     *
     * @return The result of the query. That result will never be null but can
     */
    ResultSet execute(String query) {
        return ses.execute(query);
    }

    /**
     * Executes the provided query asynchronously.
     *
     * @param statement the CQL query to execute (that can be any {@code Statement}).
     *
     * @return a future on the result of the query.
     */
    ResultSetFuture executeAsync(Statement statement) {
        return ses.executeAsync(statement);
    }
}
