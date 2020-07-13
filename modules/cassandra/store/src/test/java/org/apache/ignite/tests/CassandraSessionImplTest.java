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

package org.apache.ignite.tests;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedId;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;
import org.apache.ignite.cache.store.cassandra.session.BatchExecutionAssistant;
import org.apache.ignite.cache.store.cassandra.session.CassandraSessionImpl;
import org.apache.ignite.cache.store.cassandra.session.WrappedPreparedStatement;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CassandraSessionImplTest {

    private PreparedStatement preparedStatement1 = mockPreparedStatement();

    private PreparedStatement preparedStatement2 = mockPreparedStatement();

    private MyBoundStatement1 boundStatement1 = new MyBoundStatement1(preparedStatement1);

    private MyBoundStatement2 boundStatement2 = new MyBoundStatement2(preparedStatement2);

    @SuppressWarnings("unchecked")
    @Test
    public void executeFailureTest() {
        Session session1 = mock(Session.class);
        Session session2 = mock(Session.class);
        when(session1.prepare(any(String.class))).thenReturn(preparedStatement1);
        when(session2.prepare(any(String.class))).thenReturn(preparedStatement2);

        ResultSetFuture rsFuture = mock(ResultSetFuture.class);
        ResultSet rs = mock(ResultSet.class);
        Iterator it = mock(Iterator.class);
        when(it.hasNext()).thenReturn(true);
        when(it.next()).thenReturn(mock(Row.class));
        when(rs.iterator()).thenReturn(it);
        when(rsFuture.getUninterruptibly()).thenReturn(rs);
        /* @formatter:off */
        when(session1.executeAsync(any(Statement.class)))
            .thenThrow(new InvalidQueryException("You may have used a PreparedStatement that was created with another Cluster instance"))
            .thenThrow(new RuntimeException("this session should be refreshed / recreated"));
        when(session2.executeAsync(boundStatement1))
            .thenThrow(new InvalidQueryException("You may have used a PreparedStatement that was created with another Cluster instance"));
        when(session2.executeAsync(boundStatement2)).thenReturn(rsFuture);
        /* @formatter:on */

        Cluster cluster = mock(Cluster.class);
        when(cluster.connect()).thenReturn(session1).thenReturn(session2);
        when(session1.getCluster()).thenReturn(cluster);
        when(session2.getCluster()).thenReturn(cluster);

        Cluster.Builder builder = mock(Cluster.Builder.class);
        when(builder.build()).thenReturn(cluster);

        CassandraSessionImpl cassandraSession = new CassandraSessionImpl(builder, null,
                ConsistencyLevel.ONE, ConsistencyLevel.ONE, 0, mock(IgniteLogger.class));

        BatchExecutionAssistant<String, String> batchExecutionAssistant = new MyBatchExecutionAssistant();
        ArrayList<String> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(String.valueOf(i));
        }
        cassandraSession.execute(batchExecutionAssistant, data);

        verify(cluster, times(2)).connect();
        verify(session1, times(1)).prepare(any(String.class));
        verify(session2, times(1)).prepare(any(String.class));
        assertEquals(10, batchExecutionAssistant.processedCount());
    }

    private static PreparedStatement mockPreparedStatement() {
        PreparedStatement ps = mock(PreparedStatement.class);
        when(ps.getVariables()).thenReturn(mock(ColumnDefinitions.class));
        when(ps.getPreparedId()).thenReturn(mock(PreparedId.class));
        when(ps.getQueryString()).thenReturn("insert into xxx");
        return ps;
    }

    private class MyBatchExecutionAssistant implements BatchExecutionAssistant {

        private Set<Integer> processed = new HashSet<>();

        @Override public void process(Row row, int seqNum) {
            if (processed.contains(seqNum))
                return;

            processed.add(seqNum);
        }

        @Override public boolean alreadyProcessed(int seqNum) {
            return processed.contains(seqNum);
        }

        @Override public int processedCount() {
            return processed.size();
        }

        @Override public boolean tableExistenceRequired() {
            return false;
        }

        @Override public String getTable() {
            return null;
        }

        @Override public String getStatement() {
            return null;
        }

        @Override public BoundStatement bindStatement(PreparedStatement statement, Object obj) {
            if (statement instanceof WrappedPreparedStatement)
                statement = ((WrappedPreparedStatement)statement).getWrappedStatement();

            if (statement == preparedStatement1) {
                return boundStatement1;
            }
            else if (statement == preparedStatement2) {
                return boundStatement2;
            }

            throw new RuntimeException("unexpected");
        }

        @Override public KeyValuePersistenceSettings getPersistenceSettings() {
            return null;
        }

        @Override public String operationName() {
            return null;
        }

        @Override public Object processedData() {
            return null;
        }

    }

    private static class MyBoundStatement1 extends BoundStatement {

        MyBoundStatement1(PreparedStatement ps) {
            super(ps);
        }

    }

    private static class MyBoundStatement2 extends BoundStatement {

        MyBoundStatement2(PreparedStatement ps) {
            super(ps);
        }
    }

}
