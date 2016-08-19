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

package org.apache.ignite.cache.store.cassandra.datasource;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.AddressTranslator;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.store.cassandra.session.CassandraSession;
import org.apache.ignite.cache.store.cassandra.session.CassandraSessionImpl;

/**
 * Data source abstraction to specify configuration of the Cassandra session to be used.
 */
public class DataSource implements Externalizable {
    /** Null object, used as a replacement for those Cassandra connection options which
     * don't support serialization (RetryPolicy, LoadBalancingPolicy and etc) */
    private static final UUID NULL_OBJECT = UUID.fromString("45ffae47-3193-5910-84a2-048fe65735d9");

    /** Number of rows to immediately fetch in CQL statement execution. */
    private Integer fetchSize;

    /** Consistency level for READ operations. */
    private ConsistencyLevel readConsistency;

    /** Consistency level for WRITE operations. */
    private ConsistencyLevel writeConsistency;

    /** Username to use for authentication. */
    private String user;

    /** Password to use for authentication. */
    private String pwd;

    /** Port to use for Cassandra connection. */
    private Integer port;

    /** List of contact points to connect to Cassandra cluster. */
    private List<InetAddress> contactPoints;

    /** List of contact points with ports to connect to Cassandra cluster. */
    private List<InetSocketAddress> contactPointsWithPorts;

    /** Maximum time to wait for schema agreement before returning from a DDL query. */
    private Integer maxSchemaAgreementWaitSeconds;

    /** The native protocol version to use. */
    private Integer protoVer;

    /** Compression to use for the transport. */
    private String compression;

    /** Use SSL for communications with Cassandra. */
    private Boolean useSSL;

    /** Enables metrics collection. */
    private Boolean collectMetrix;

    /** Enables JMX reporting of the metrics. */
    private Boolean jmxReporting;

    /** Credentials to use for authentication. */
    private Credentials creds;

    /** Load balancing policy to use. */
    private LoadBalancingPolicy loadBalancingPlc;

    /** Reconnection policy to use. */
    private ReconnectionPolicy reconnectionPlc;

    /** Retry policy to use. */
    private RetryPolicy retryPlc;

    /** Address translator to use. */
    private AddressTranslator addrTranslator;

    /** Speculative execution policy to use. */
    private SpeculativeExecutionPolicy speculativeExecutionPlc;

    /** Authentication provider to use. */
    private AuthProvider authProvider;

    /** SSL options to use. */
    private SSLOptions sslOptions;

    /** Connection pooling options to use. */
    private PoolingOptions poolingOptions;

    /** Socket options to use. */
    private SocketOptions sockOptions;

    /** Netty options to use for connection. */
    private NettyOptions nettyOptions;

    /** Cassandra session wrapper instance. */
    private volatile CassandraSession ses;

    /**
     * Sets user name to use for authentication.
     *
     * @param user user name
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setUser(String user) {
        this.user = user;

        invalidate();
    }

    /**
     * Sets password to use for authentication.
     *
     * @param pwd password
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setPassword(String pwd) {
        this.pwd = pwd;

        invalidate();
    }

    /**
     * Sets port to use for Cassandra connection.
     *
     * @param port port
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setPort(int port) {
        this.port = port;

        invalidate();
    }

    /**
     * Sets list of contact points to connect to Cassandra cluster.
     *
     * @param points contact points
     */
    public void setContactPoints(String... points) {
        if (points == null || points.length == 0)
            return;

        for (String point : points) {
            if (point.contains(":")) {
                if (contactPointsWithPorts == null)
                    contactPointsWithPorts = new LinkedList<>();

                String[] chunks = point.split(":");

                try {
                    contactPointsWithPorts.add(InetSocketAddress.createUnresolved(chunks[0].trim(), Integer.parseInt(chunks[1].trim())));
                }
                catch (Throwable e) {
                    throw new IllegalArgumentException("Incorrect contact point '" + point + "' specified for Cassandra cache storage", e);
                }
            }
            else {
                if (contactPoints == null)
                    contactPoints = new LinkedList<>();

                try {
                    contactPoints.add(InetAddress.getByName(point));
                }
                catch (Throwable e) {
                    throw new IllegalArgumentException("Incorrect contact point '" + point + "' specified for Cassandra cache storage", e);
                }
            }
        }

        invalidate();
    }

    /** Sets maximum time to wait for schema agreement before returning from a DDL query. */
    @SuppressWarnings("UnusedDeclaration")
    public void setMaxSchemaAgreementWaitSeconds(int seconds) {
        maxSchemaAgreementWaitSeconds = seconds;

        invalidate();
    }

    /**
     * Sets the native protocol version to use.
     *
     * @param ver version number
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setProtocolVersion(int ver) {
        protoVer = ver;

        invalidate();
    }

    /**
     * Sets compression algorithm to use for the transport.
     *
     * @param compression Compression algorithm.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setCompression(String compression) {
        this.compression = compression == null || compression.trim().isEmpty() ? null : compression.trim();

        try {
            if (this.compression != null)
                ProtocolOptions.Compression.valueOf(this.compression);
        }
        catch (Throwable e) {
            throw new IgniteException("Incorrect compression '" + compression + "' specified for Cassandra connection", e);
        }

        invalidate();
    }

    /**
     * Enables SSL for communications with Cassandra.
     *
     * @param use Flag to enable/disable SSL.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setUseSSL(boolean use) {
        useSSL = use;

        invalidate();
    }

    /**
     * Enables metrics collection.
     *
     * @param collect Flag to enable/disable metrics collection.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setCollectMetrix(boolean collect) {
        collectMetrix = collect;

        invalidate();
    }

    /**
     * Enables JMX reporting of the metrics.
     *
     * @param enableReporting Flag to enable/disable JMX reporting.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setJmxReporting(boolean enableReporting) {
        jmxReporting = enableReporting;

        invalidate();
    }

    /**
     * Sets number of rows to immediately fetch in CQL statement execution.
     *
     * @param size Number of rows to fetch.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setFetchSize(int size) {
        fetchSize = size;

        invalidate();
    }

    /**
     * Set consistency level for READ operations.
     *
     * @param level Consistency level.
     */
    public void setReadConsistency(String level) {
        readConsistency = parseConsistencyLevel(level);

        invalidate();
    }

    /**
     * Set consistency level for WRITE operations.
     *
     * @param level Consistency level.
     */
    public void setWriteConsistency(String level) {
        writeConsistency = parseConsistencyLevel(level);

        invalidate();
    }

    /**
     * Sets credentials to use for authentication.
     *
     * @param creds Credentials.
     */
    public void setCredentials(Credentials creds) {
        this.creds = creds;

        invalidate();
    }

    /**
     * Sets load balancing policy.
     *
     * @param plc Load balancing policy.
     */
    public void setLoadBalancingPolicy(LoadBalancingPolicy plc) {
        this.loadBalancingPlc = plc;

        invalidate();
    }

    /**
     * Sets reconnection policy.
     *
     * @param plc Reconnection policy.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setReconnectionPolicy(ReconnectionPolicy plc) {
        this.reconnectionPlc = plc;

        invalidate();
    }

    /**
     * Sets retry policy.
     *
     * @param plc Retry policy.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setRetryPolicy(RetryPolicy plc) {
        this.retryPlc = plc;

        invalidate();
    }

    /**
     * Sets address translator.
     *
     * @param translator Address translator.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setAddressTranslator(AddressTranslator translator) {
        this.addrTranslator = translator;

        invalidate();
    }

    /**
     * Sets speculative execution policy.
     *
     * @param plc Speculative execution policy.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setSpeculativeExecutionPolicy(SpeculativeExecutionPolicy plc) {
        this.speculativeExecutionPlc = plc;

        invalidate();
    }

    /**
     * Sets authentication provider.
     *
     * @param provider Authentication provider.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setAuthProvider(AuthProvider provider) {
        this.authProvider = provider;

        invalidate();
    }

    /**
     * Sets SSL options.
     *
     * @param options SSL options.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setSslOptions(SSLOptions options) {
        this.sslOptions = options;

        invalidate();
    }

    /**
     * Sets pooling options.
     *
     * @param options pooling options to use.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setPoolingOptions(PoolingOptions options) {
        this.poolingOptions = options;

        invalidate();
    }

    /**
     * Sets socket options to use.
     *
     * @param options Socket options.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setSocketOptions(SocketOptions options) {
        this.sockOptions = options;

        invalidate();
    }

    /**
     * Sets netty options to use.
     *
     * @param options netty options.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setNettyOptions(NettyOptions options) {
        this.nettyOptions = options;

        invalidate();
    }

    /**
     * Creates Cassandra session wrapper if it wasn't created yet and returns it
     *
     * @param log logger
     * @return Cassandra session wrapper
     */
    @SuppressWarnings("deprecation")
    public synchronized CassandraSession session(IgniteLogger log) {
        if (ses != null)
            return ses;

        Cluster.Builder builder = Cluster.builder();

        if (user != null)
            builder = builder.withCredentials(user, pwd);

        if (port != null)
            builder = builder.withPort(port);

        if (contactPoints != null)
            builder = builder.addContactPoints(contactPoints);

        if (contactPointsWithPorts != null)
            builder = builder.addContactPointsWithPorts(contactPointsWithPorts);

        if (maxSchemaAgreementWaitSeconds != null)
            builder = builder.withMaxSchemaAgreementWaitSeconds(maxSchemaAgreementWaitSeconds);

        if (protoVer != null)
            builder = builder.withProtocolVersion(ProtocolVersion.fromInt(protoVer));

        if (compression != null) {
            try {
                builder = builder.withCompression(ProtocolOptions.Compression.valueOf(compression.trim().toLowerCase()));
            }
            catch (IllegalArgumentException e) {
                throw new IgniteException("Incorrect compression option '" + compression + "' specified for Cassandra connection", e);
            }
        }

        if (useSSL != null && useSSL)
            builder = builder.withSSL();

        if (sslOptions != null)
            builder = builder.withSSL(sslOptions);

        if (collectMetrix != null && !collectMetrix)
            builder = builder.withoutMetrics();

        if (jmxReporting != null && !jmxReporting)
            builder = builder.withoutJMXReporting();

        if (creds != null)
            builder = builder.withCredentials(creds.getUser(), creds.getPassword());

        if (loadBalancingPlc != null)
            builder = builder.withLoadBalancingPolicy(loadBalancingPlc);

        if (reconnectionPlc != null)
            builder = builder.withReconnectionPolicy(reconnectionPlc);

        if (retryPlc != null)
            builder = builder.withRetryPolicy(retryPlc);

        if (addrTranslator != null)
            builder = builder.withAddressTranslator(addrTranslator);

        if (speculativeExecutionPlc != null)
            builder = builder.withSpeculativeExecutionPolicy(speculativeExecutionPlc);

        if (authProvider != null)
            builder = builder.withAuthProvider(authProvider);

        if (poolingOptions != null)
            builder = builder.withPoolingOptions(poolingOptions);

        if (sockOptions != null)
            builder = builder.withSocketOptions(sockOptions);

        if (nettyOptions != null)
            builder = builder.withNettyOptions(nettyOptions);

        return ses = new CassandraSessionImpl(builder, fetchSize, readConsistency, writeConsistency, log);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(fetchSize);
        out.writeObject(readConsistency);
        out.writeObject(writeConsistency);
        out.writeObject(user);
        out.writeObject(pwd);
        out.writeObject(port);
        out.writeObject(contactPoints);
        out.writeObject(contactPointsWithPorts);
        out.writeObject(maxSchemaAgreementWaitSeconds);
        out.writeObject(protoVer);
        out.writeObject(compression);
        out.writeObject(useSSL);
        out.writeObject(collectMetrix);
        out.writeObject(jmxReporting);
        out.writeObject(creds);
        writeObject(out, loadBalancingPlc);
        writeObject(out, reconnectionPlc);
        writeObject(out, addrTranslator);
        writeObject(out, speculativeExecutionPlc);
        writeObject(out, authProvider);
        writeObject(out, sslOptions);
        writeObject(out, poolingOptions);
        writeObject(out, sockOptions);
        writeObject(out, nettyOptions);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        fetchSize = (Integer)in.readObject();
        readConsistency = (ConsistencyLevel)in.readObject();
        writeConsistency = (ConsistencyLevel)in.readObject();
        user = (String)in.readObject();
        pwd = (String)in.readObject();
        port = (Integer)in.readObject();
        contactPoints = (List<InetAddress>)in.readObject();
        contactPointsWithPorts = (List<InetSocketAddress>)in.readObject();
        maxSchemaAgreementWaitSeconds = (Integer)in.readObject();
        protoVer = (Integer)in.readObject();
        compression = (String)in.readObject();
        useSSL = (Boolean)in.readObject();
        collectMetrix = (Boolean)in.readObject();
        jmxReporting = (Boolean)in.readObject();
        creds = (Credentials)in.readObject();
        loadBalancingPlc = (LoadBalancingPolicy)readObject(in);
        reconnectionPlc = (ReconnectionPolicy)readObject(in);
        addrTranslator = (AddressTranslator)readObject(in);
        speculativeExecutionPlc = (SpeculativeExecutionPolicy)readObject(in);
        authProvider = (AuthProvider)readObject(in);
        sslOptions = (SSLOptions)readObject(in);
        poolingOptions = (PoolingOptions)readObject(in);
        sockOptions = (SocketOptions)readObject(in);
        nettyOptions = (NettyOptions)readObject(in);
    }

    /**
     * Helper method used to serialize class members
     * @param out the stream to write the object to
     * @param obj the object to be written
     * @throws IOException Includes any I/O exceptions that may occur
     */
    private void writeObject(ObjectOutput out, Object obj) throws IOException {
        out.writeObject(obj == null || !(obj instanceof Serializable) ? NULL_OBJECT : obj);
    }

    /**
     * Helper method used to deserialize class members
     * @param in the stream to read data from in order to restore the object
     * @throws IOException Includes any I/O exceptions that may occur
     * @throws ClassNotFoundException If the class for an object being restored cannot be found
     * @return deserialized object
     */
    private Object readObject(ObjectInput in) throws IOException, ClassNotFoundException {
        Object obj = in.readObject();
        return NULL_OBJECT.equals(obj) ? null : obj;
    }

    /**
     * Parses consistency level provided as string.
     *
     * @param level consistency level string.
     *
     * @return consistency level.
     */
    private ConsistencyLevel parseConsistencyLevel(String level) {
        if (level == null)
            return null;

        try {
            return ConsistencyLevel.valueOf(level.trim().toUpperCase());
        }
        catch (Throwable e) {
            throw new IgniteException("Incorrect consistency level '" + level + "' specified for Cassandra connection", e);
        }
    }

    /**
     * Invalidates session.
     */
    private synchronized void invalidate() {
        ses = null;
    }
}
