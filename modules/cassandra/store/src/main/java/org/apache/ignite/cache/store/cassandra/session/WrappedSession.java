/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
