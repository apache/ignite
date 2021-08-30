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

import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import org.apache.ignite.cache.store.cassandra.datasource.Credentials;
import org.apache.ignite.cache.store.cassandra.datasource.DataSource;
import org.apache.ignite.cache.store.cassandra.serializer.JavaSerializer;
import org.apache.ignite.tests.utils.CassandraAdminCredentials;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Test for datasource serialization.
 */
public class DatasourceSerializationTest {
    /**
     * Sample class for serialization test.
     */
    private static class MyLoadBalancingPolicy implements LoadBalancingPolicy, Serializable {
        /** */
        private transient LoadBalancingPolicy plc = new TokenAwarePolicy(new RoundRobinPolicy());

        /** {@inheritDoc} */
        @Override public void init(Cluster cluster, Collection<Host> hosts) {
            plc.init(cluster, hosts);
        }

        /** {@inheritDoc} */
        @Override public HostDistance distance(Host host) {
            return plc.distance(host);
        }

        /** {@inheritDoc} */
        @Override public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
            return plc.newQueryPlan(loggedKeyspace, statement);
        }

        /** {@inheritDoc} */
        @Override public void onAdd(Host host) {
            plc.onAdd(host);
        }

        /** {@inheritDoc} */
        @Override public void onUp(Host host) {
            plc.onUp(host);
        }

        /** {@inheritDoc} */
        @Override public void onDown(Host host) {
            plc.onDown(host);
        }

        /** {@inheritDoc} */
        @Override public void onRemove(Host host) {
            plc.onRemove(host);
        }

        /** {@inheritDoc} */
        @Override public void close() {
            plc.close();
        }
    }

    /**
     * Serialization test.
     */
    @Test
    public void serializationTest() {
        DataSource src = new DataSource();

        Credentials cred = new CassandraAdminCredentials();
        String[] points = new String[]{"127.0.0.1", "10.0.0.2", "10.0.0.3"};
        LoadBalancingPolicy plc = new MyLoadBalancingPolicy();

        src.setCredentials(cred);
        src.setContactPoints(points);
        src.setReadConsistency("ONE");
        src.setWriteConsistency("QUORUM");
        src.setLoadBalancingPolicy(plc);

        JavaSerializer serializer = new JavaSerializer();

        ByteBuffer buff = serializer.serialize(src);
        DataSource _src = (DataSource)serializer.deserialize(buff);

        Credentials _cred = (Credentials)getFieldValue(_src, "creds");
        List<InetAddress> _points = (List<InetAddress>)getFieldValue(_src, "contactPoints");
        ConsistencyLevel _readCons = (ConsistencyLevel)getFieldValue(_src, "readConsistency");
        ConsistencyLevel _writeCons = (ConsistencyLevel)getFieldValue(_src, "writeConsistency");
        LoadBalancingPolicy _plc = (LoadBalancingPolicy)getFieldValue(_src, "loadBalancingPlc");

        assertTrue("Incorrectly serialized/deserialized credentials for Cassandra DataSource",
            cred.getPassword().equals(_cred.getPassword()) && cred.getUser().equals(_cred.getUser()));

        assertTrue("Incorrectly serialized/deserialized contact points for Cassandra DataSource",
            "/127.0.0.1".equals(_points.get(0).toString()) &&
            "/10.0.0.2".equals(_points.get(1).toString()) &&
            "/10.0.0.3".equals(_points.get(2).toString()));

        assertTrue("Incorrectly serialized/deserialized consistency levels for Cassandra DataSource",
            ConsistencyLevel.ONE == _readCons && ConsistencyLevel.QUORUM == _writeCons);

        assertTrue("Incorrectly serialized/deserialized load balancing policy for Cassandra DataSource",
            _plc instanceof MyLoadBalancingPolicy);
    }

    /**
     * @param obj Object.
     * @param field Field name.
     * @return Field value.
     */
    private Object getFieldValue(Object obj, String field) {
        try {
            Field f = obj.getClass().getDeclaredField(field);

            f.setAccessible(true);

            return f.get(obj);
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to get field '" + field + "' value", e);
        }
    }
}
