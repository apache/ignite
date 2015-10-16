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

package org.apache.ignite.cache.store.cassandra.utils.datasource;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.AddressTranslater;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.store.cassandra.utils.session.CassandraSession;
import org.apache.ignite.cache.store.cassandra.utils.session.CassandraSessionImpl;

/**
 * Data source abstraction to specify configuration of the Cassandra session to be used
 */
public class DataSource {
    private Integer fetchSize;
    private ConsistencyLevel readConsistency;
    private ConsistencyLevel writeConsistency;

    private String user;
    private String password;
    private Integer port;
    private List<InetAddress> contactPoints;
    private List<InetSocketAddress> contactPointsWithPorts;
    private Integer maxSchemaAgreementWaitSeconds;
    private Integer protocolVersion;
    private String compression;
    private Boolean useSSL;
    private Boolean collectMetrix;
    private Boolean jmxReporting;

    private Credentials credentials;
    private LoadBalancingPolicy loadBalancingPolicy;
    private ReconnectionPolicy reconnectionPolicy;
    private RetryPolicy retryPolicy;
    private AddressTranslater addressTranslater;
    private SpeculativeExecutionPolicy speculativeExecutionPolicy;
    private AuthProvider authProvider;
    private SSLOptions sslOptions;
    private PoolingOptions poolingOptions;
    private SocketOptions socketOptions;
    private NettyOptions nettyOptions;

    private volatile CassandraSession session;

    @SuppressWarnings("UnusedDeclaration")
    public void setUser(String user) {
        this.user = user;
        invalidate();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setPassword(String password) {
        this.password = password;
        invalidate();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setPort(int port) {
        this.port = port;
        invalidate();
    }

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

    @SuppressWarnings("UnusedDeclaration")
    public void setMaxSchemaAgreementWaitSeconds(int seconds) {
        maxSchemaAgreementWaitSeconds = seconds;
        invalidate();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setProtocolVersion(int version) {
        protocolVersion = version;
        invalidate();
    }

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

    @SuppressWarnings("UnusedDeclaration")
    public void setUseSSL(boolean use) {
        useSSL = use;
        invalidate();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setCollectMetrix(boolean collect) {
        collectMetrix = collect;
        invalidate();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setJmxReporting(boolean enableReporting) {
        jmxReporting = enableReporting;
        invalidate();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setFetchSize(int size) {
        fetchSize = size;
        invalidate();
    }

    public void setReadConsistency(String level) {
        readConsistency = parseConsistencyLevel(level);
        invalidate();
    }

    public void setWriteConsistency(String level) {
        writeConsistency = parseConsistencyLevel(level);
        invalidate();
    }

    public void setCredentials(Credentials credentials) {
        this.credentials = credentials;
        invalidate();
    }

    public void setLoadBalancingPolicy(LoadBalancingPolicy policy) {
        this.loadBalancingPolicy = policy;
        invalidate();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setReconnectionPolicy(ReconnectionPolicy policy) {
        this.reconnectionPolicy = policy;
        invalidate();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setRetryPolicy(RetryPolicy policy) {
        this.retryPolicy = policy;
        invalidate();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setAddressTranslater(AddressTranslater translater) {
        this.addressTranslater = translater;
        invalidate();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setSpeculativeExecutionPolicy(SpeculativeExecutionPolicy policy) {
        this.speculativeExecutionPolicy = policy;
        invalidate();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setAuthProvider(AuthProvider provider) {
        this.authProvider = provider;
        invalidate();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setSslOptions(SSLOptions options) {
        this.sslOptions = options;
        invalidate();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setPoolingOptions(PoolingOptions options) {
        this.poolingOptions = options;
        invalidate();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setSocketOptions(SocketOptions options) {
        this.socketOptions = options;
        invalidate();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setNettyOptions(NettyOptions options) {
        this.nettyOptions = options;
        invalidate();
    }

    @SuppressWarnings("deprecation")
    public synchronized CassandraSession session(IgniteLogger logger) {
        if (session != null)
            return session;

        Cluster.Builder builder = Cluster.builder();

        if (user != null)
            builder = builder.withCredentials(user, password);

        if (port != null)
            builder = builder.withPort(port);

        if (contactPoints != null)
            builder = builder.addContactPoints(contactPoints);

        if (contactPointsWithPorts != null)
            builder = builder.addContactPointsWithPorts(contactPointsWithPorts);

        if (maxSchemaAgreementWaitSeconds != null)
            builder = builder.withMaxSchemaAgreementWaitSeconds(maxSchemaAgreementWaitSeconds);

        if (protocolVersion != null)
            builder = builder.withProtocolVersion(protocolVersion);

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

        if (credentials != null)
            builder = builder.withCredentials(credentials.getUser(), credentials.getPassword());

        if (loadBalancingPolicy != null)
            builder = builder.withLoadBalancingPolicy(loadBalancingPolicy);

        if (reconnectionPolicy != null)
            builder = builder.withReconnectionPolicy(reconnectionPolicy);

        if (retryPolicy != null)
            builder = builder.withRetryPolicy(retryPolicy);

        if (addressTranslater != null)
            builder = builder.withAddressTranslater(addressTranslater);

        if (speculativeExecutionPolicy != null)
            builder = builder.withSpeculativeExecutionPolicy(speculativeExecutionPolicy);

        if (authProvider != null)
            builder = builder.withAuthProvider(authProvider);

        if (poolingOptions != null)
            builder = builder.withPoolingOptions(poolingOptions);

        if (socketOptions != null)
            builder = builder.withSocketOptions(socketOptions);

        if (nettyOptions != null)
            builder = builder.withNettyOptions(nettyOptions);

        return session = new CassandraSessionImpl(builder, fetchSize, readConsistency, writeConsistency, logger);
    }

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

    private synchronized void invalidate() {
        session = null;
    }

}
