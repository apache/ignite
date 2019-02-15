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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedId;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.policies.RetryPolicy;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Simple wrapper providing access to Cassandra prepared statement and generation of Cassandra
 * session which was used to create this statement
 */
public class WrappedPreparedStatement implements PreparedStatement {
    /** Prepared statement. **/
    private final PreparedStatement st;

    /** Generation of Cassandra session which was used to prepare this statement. **/
    final long generation;

    /**
     * Constructor.
     *
     * @param st Prepared statement.
     * @param generation Generation of Cassandra session used to prepare this statement.
     */
    WrappedPreparedStatement(PreparedStatement st, long generation) {
        this.st = st;
        this.generation = generation;
    }

    /**
     * Getter for wrapped statement.
     *
     * @return Wrapped original statement.
     */
    public PreparedStatement getWrappedStatement() {
        return st;
    }

    /** {@inheritDoc} */
    @Override public ColumnDefinitions getVariables() {
        return st.getVariables();
    }

    /** {@inheritDoc} */
    @Override public BoundStatement bind(Object... values) {
        return st.bind(values);
    }

    /** {@inheritDoc} */
    @Override public BoundStatement bind() {
        return st.bind();
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement setRoutingKey(ByteBuffer routingKey) {
        return st.setRoutingKey(routingKey);
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement setRoutingKey(ByteBuffer... routingKeyComponents) {
        return st.setRoutingKey(routingKeyComponents);
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer getRoutingKey() {
        return st.getRoutingKey();
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement setConsistencyLevel(ConsistencyLevel consistency) {
        return st.setConsistencyLevel(consistency);
    }

    /** {@inheritDoc} */
    @Override public ConsistencyLevel getConsistencyLevel() {
        return st.getConsistencyLevel();
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement setSerialConsistencyLevel(ConsistencyLevel serialConsistency) {
        return st.setSerialConsistencyLevel(serialConsistency);
    }

    /** {@inheritDoc} */
    @Override public ConsistencyLevel getSerialConsistencyLevel() {
        return st.getSerialConsistencyLevel();
    }

    /** {@inheritDoc} */
    @Override public String getQueryString() {
        return st.getQueryString();
    }

    /** {@inheritDoc} */
    @Override public String getQueryKeyspace() {
        return st.getQueryKeyspace();
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement enableTracing() {
        return st.enableTracing();
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement disableTracing() {
        return st.disableTracing();
    }

    /** {@inheritDoc} */
    @Override public boolean isTracing() {
        return st.isTracing();
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement setRetryPolicy(RetryPolicy policy) {
        return st.setRetryPolicy(policy);
    }

    /** {@inheritDoc} */
    @Override public RetryPolicy getRetryPolicy() {
        return st.getRetryPolicy();
    }

    /** {@inheritDoc} */
    @Override public PreparedId getPreparedId() {
        return st.getPreparedId();
    }

    /** {@inheritDoc} */
    @Override public Map<String, ByteBuffer> getIncomingPayload() {
        return st.getIncomingPayload();
    }

    /** {@inheritDoc} */
    @Override public Map<String, ByteBuffer> getOutgoingPayload() {
        return st.getOutgoingPayload();
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement setOutgoingPayload(Map<String, ByteBuffer> payload) {
        return st.setOutgoingPayload(payload);
    }

    /** {@inheritDoc} */
    @Override public CodecRegistry getCodecRegistry() {
        return st.getCodecRegistry();
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement setIdempotent(Boolean idempotent) {
        return st.setIdempotent(idempotent);
    }

    /** {@inheritDoc} */
    @Override public Boolean isIdempotent() {
        return st.isIdempotent();
    }
}
