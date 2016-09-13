package org.apache.ignite.tests;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import org.apache.ignite.cache.store.cassandra.datasource.Credentials;
import org.apache.ignite.cache.store.cassandra.datasource.DataSource;
import org.apache.ignite.cache.store.cassandra.serializer.JavaSerializer;
import org.apache.ignite.tests.utils.CassandraAdminCredentials;
import org.junit.Test;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class DatasourceSerializationTest {
    private static class MyLoadBalancingPolicy implements LoadBalancingPolicy, Serializable {
        private transient LoadBalancingPolicy policy = new TokenAwarePolicy(new RoundRobinPolicy());

        @Override public void init(Cluster cluster, Collection<Host> hosts) {
            policy.init(cluster, hosts);
        }

        @Override public HostDistance distance(Host host) {
            return policy.distance(host);
        }

        @Override public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
            return policy.newQueryPlan(loggedKeyspace, statement);
        }

        @Override public void onAdd(Host host) {
            policy.onAdd(host);
        }

        @Override public void onUp(Host host) {
            policy.onUp(host);
        }

        @Override public void onDown(Host host) {
            policy.onDown(host);
        }

        @Override public void onRemove(Host host) {
            policy.onRemove(host);
        }

        @Override public void close() {
            policy.close();
        }
    }

    @Test public void serializationTest() {
        DataSource src = new DataSource();

        Credentials cred = new CassandraAdminCredentials();
        String[] points = new String[]{"127.0.0.1", "10.0.0.2", "10.0.0.3"};
        LoadBalancingPolicy policy = new MyLoadBalancingPolicy();

        src.setCredentials(cred);
        src.setContactPoints(points);
        src.setReadConsistency("ONE");
        src.setWriteConsistency("QUORUM");
        src.setLoadBalancingPolicy(policy);

        JavaSerializer serializer = new JavaSerializer();

        ByteBuffer buff = serializer.serialize(src);
        DataSource _src = (DataSource)serializer.deserialize(buff);

        Credentials _cred = (Credentials)getFieldValue(_src, "creds");
        List<InetAddress> _points = (List<InetAddress>)getFieldValue(_src, "contactPoints");
        ConsistencyLevel _readCons = (ConsistencyLevel)getFieldValue(_src, "readConsistency");
        ConsistencyLevel _writeCons = (ConsistencyLevel)getFieldValue(_src, "writeConsistency");
        LoadBalancingPolicy _policy = (LoadBalancingPolicy)getFieldValue(_src, "loadBalancingPlc");

        if (!cred.getPassword().equals(_cred.getPassword()) || !cred.getUser().equals(_cred.getUser()))
            throw new RuntimeException("Incorrectly serialized/deserialized credentials for Cassandra DataSource");

        if (!_points.get(0).toString().equals("/127.0.0.1") ||
            !_points.get(1).toString().equals("/10.0.0.2") ||
            !_points.get(2).toString().equals("/10.0.0.3")) {
            throw new RuntimeException("Incorrectly serialized/deserialized contact points for Cassandra DataSource");
        }

        if (!ConsistencyLevel.ONE.equals(_readCons) || !ConsistencyLevel.QUORUM.equals(_writeCons))
            throw new RuntimeException("Incorrectly serialized/deserialized consistency levels for Cassandra DataSource");

        if (!(_policy instanceof MyLoadBalancingPolicy))
            throw new RuntimeException("Incorrectly serialized/deserialized load balancing policy for Cassandra DataSource");
    }

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
