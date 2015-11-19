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
    /** TODO IGNITE-1371: add comment */
    private Integer fetchSize;

    /** TODO IGNITE-1371: add comment */
    private ConsistencyLevel readConsistency;

    /** TODO IGNITE-1371: add comment */
    private ConsistencyLevel writeConsistency;

    /** TODO IGNITE-1371: add comment */
    private String user;

    /** TODO IGNITE-1371: add comment */
    private String pwd;

    /** TODO IGNITE-1371: add comment */
    private Integer port;

    /** TODO IGNITE-1371: add comment */
    private List<InetAddress> contactPoints;

    /** TODO IGNITE-1371: add comment */
    private List<InetSocketAddress> contactPointsWithPorts;

    /** TODO IGNITE-1371: add comment */
    private Integer maxSchemaAgreementWaitSeconds;

    /** TODO IGNITE-1371: add comment */
    private Integer protoVer;

    /** TODO IGNITE-1371: add comment */
    private String compression;

    /** TODO IGNITE-1371: add comment */
    private Boolean useSSL;

    /** TODO IGNITE-1371: add comment */
    private Boolean collectMetrix;

    /** TODO IGNITE-1371: add comment */
    private Boolean jmxReporting;

    /** TODO IGNITE-1371: add comment */
    private Credentials creds;

    /** TODO IGNITE-1371: add comment */
    private LoadBalancingPolicy loadBalancingPlc;

    /** TODO IGNITE-1371: add comment */
    private ReconnectionPolicy reconnectionPlc;

    /** TODO IGNITE-1371: add comment */
    private RetryPolicy retryPlc;

    /** TODO IGNITE-1371: add comment */
    private AddressTranslater addrTranslater;

    /** TODO IGNITE-1371: add comment */
    private SpeculativeExecutionPolicy speculativeExecutionPlc;

    /** TODO IGNITE-1371: add comment */
    private AuthProvider authProvider;

    /** TODO IGNITE-1371: add comment */
    private SSLOptions sslOptions;

    /** TODO IGNITE-1371: add comment */
    private PoolingOptions poolingOptions;

    /** TODO IGNITE-1371: add comment */
    private SocketOptions sockOptions;

    /** TODO IGNITE-1371: add comment */
    private NettyOptions nettyOptions;

    /** TODO IGNITE-1371: add comment */
    private volatile CassandraSession ses;

    /** TODO IGNITE-1371: add comment */
    @SuppressWarnings("UnusedDeclaration")
    public void setUser(String user) {
        this.user = user;

        invalidate();
    }

    /** TODO IGNITE-1371: add comment */
    @SuppressWarnings("UnusedDeclaration")
    public void setPassword(String pwd) {
        this.pwd = pwd;

        invalidate();
    }

    /** TODO IGNITE-1371: add comment */
    @SuppressWarnings("UnusedDeclaration")
    public void setPort(int port) {
        this.port = port;

        invalidate();
    }

    /** TODO IGNITE-1371: add comment */
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

    /** TODO IGNITE-1371: add comment */
    @SuppressWarnings("UnusedDeclaration")
    public void setMaxSchemaAgreementWaitSeconds(int seconds) {
        maxSchemaAgreementWaitSeconds = seconds;

        invalidate();
    }

    /** TODO IGNITE-1371: add comment */
    @SuppressWarnings("UnusedDeclaration")
    public void setProtocolVersion(int ver) {
        protoVer = ver;

        invalidate();
    }

    /** TODO IGNITE-1371: add comment */
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

    /** TODO IGNITE-1371: add comment */
    @SuppressWarnings("UnusedDeclaration")
    public void setUseSSL(boolean use) {
        useSSL = use;

        invalidate();
    }

    /** TODO IGNITE-1371: add comment */
    @SuppressWarnings("UnusedDeclaration")
    public void setCollectMetrix(boolean collect) {
        collectMetrix = collect;

        invalidate();
    }

    /** TODO IGNITE-1371: add comment */
    @SuppressWarnings("UnusedDeclaration")
    public void setJmxReporting(boolean enableReporting) {
        jmxReporting = enableReporting;

        invalidate();
    }

    /** TODO IGNITE-1371: add comment */
    @SuppressWarnings("UnusedDeclaration")
    public void setFetchSize(int size) {
        fetchSize = size;

        invalidate();
    }

    /** TODO IGNITE-1371: add comment */
    public void setReadConsistency(String level) {
        readConsistency = parseConsistencyLevel(level);

        invalidate();
    }

    /** TODO IGNITE-1371: add comment */
    public void setWriteConsistency(String level) {
        writeConsistency = parseConsistencyLevel(level);

        invalidate();
    }

    /** TODO IGNITE-1371: add comment */
    public void setCredentials(Credentials creds) {
        this.creds = creds;

        invalidate();
    }

    /** TODO IGNITE-1371: add comment */
    public void setLoadBalancingPolicy(LoadBalancingPolicy plc) {
        this.loadBalancingPlc = plc;

        invalidate();
    }

    /** TODO IGNITE-1371: add comment */
    @SuppressWarnings("UnusedDeclaration")
    public void setReconnectionPolicy(ReconnectionPolicy plc) {
        this.reconnectionPlc = plc;

        invalidate();
    }

    /** TODO IGNITE-1371: add comment */
    @SuppressWarnings("UnusedDeclaration")
    public void setRetryPolicy(RetryPolicy plc) {
        this.retryPlc = plc;

        invalidate();
    }

    /** TODO IGNITE-1371: add comment */
    @SuppressWarnings("UnusedDeclaration")
    public void setAddressTranslater(AddressTranslater translater) {
        this.addrTranslater = translater;

        invalidate();
    }

    /** TODO IGNITE-1371: add comment */
    @SuppressWarnings("UnusedDeclaration")
    public void setSpeculativeExecutionPolicy(SpeculativeExecutionPolicy plc) {
        this.speculativeExecutionPlc = plc;
        invalidate();
    }

    /** TODO IGNITE-1371: add comment */
    @SuppressWarnings("UnusedDeclaration")
    public void setAuthProvider(AuthProvider provider) {
        this.authProvider = provider;
        invalidate();
    }

    /** TODO IGNITE-1371: add comment */
    @SuppressWarnings("UnusedDeclaration")
    public void setSslOptions(SSLOptions options) {
        this.sslOptions = options;
        invalidate();
    }

    /** TODO IGNITE-1371: add comment */
    @SuppressWarnings("UnusedDeclaration")
    public void setPoolingOptions(PoolingOptions options) {
        this.poolingOptions = options;
        invalidate();
    }

    /** TODO IGNITE-1371: add comment */
    @SuppressWarnings("UnusedDeclaration")
    public void setSocketOptions(SocketOptions options) {
        this.sockOptions = options;
        invalidate();
    }

    /** TODO IGNITE-1371: add comment */
    @SuppressWarnings("UnusedDeclaration")
    public void setNettyOptions(NettyOptions options) {
        this.nettyOptions = options;
        invalidate();
    }

    /** TODO IGNITE-1371: add comment */
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
            builder = builder.withProtocolVersion(protoVer);

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

        if (addrTranslater != null)
            builder = builder.withAddressTranslater(addrTranslater);

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

    /** TODO IGNITE-1371: add comment */
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

    /** TODO IGNITE-1371: add comment */
    private synchronized void invalidate() {
        ses = null;
    }
}
