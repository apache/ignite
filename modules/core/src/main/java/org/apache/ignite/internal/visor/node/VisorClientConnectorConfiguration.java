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

package org.apache.ignite.internal.visor.node;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;

/**
 * Data transfer object for client connector configuration.
 */
public class VisorClientConnectorConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Host. */
    private String host;

    /** Port. */
    private int port;

    /** Port range. */
    private int portRange;

    /** Max number of opened cursors per connection. */
    private int maxOpenCursorsPerConn;

    /** Socket send buffer size. */
    private int sockSndBufSize;

    /** Socket receive buffer size. */
    private int sockRcvBufSize;

    /** TCP no delay. */
    private boolean tcpNoDelay;

    /** Thread pool size. */
    private int threadPoolSize;

    /** Idle timeout. */
    private long idleTimeout;

    /** JDBC connections enabled flag. */
    private boolean jdbcEnabled;

    /** ODBC connections enabled flag. */
    private boolean odbcEnabled;

    /** JDBC connections enabled flag. */
    private boolean thinCliEnabled;

    /** SSL enable flag, default is disabled. */
    private boolean sslEnabled;

    /** If to use SSL context factory from Ignite configuration. */
    private boolean useIgniteSslCtxFactory;

    /** SSL need client auth flag. */
    private boolean sslClientAuth;

    /** SSL connection factory class name. */
    private String sslCtxFactory;

    /**
     * Default constructor.
     */
    public VisorClientConnectorConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for Sql connector configuration.
     *
     * @param cfg Sql connector configuration.
     */
    public VisorClientConnectorConfiguration(ClientConnectorConfiguration cfg) {
        host = cfg.getHost();
        port = cfg.getPort();
        portRange = cfg.getPortRange();
        maxOpenCursorsPerConn = cfg.getMaxOpenCursorsPerConnection();
        sockSndBufSize = cfg.getSocketSendBufferSize();
        sockRcvBufSize = cfg.getSocketReceiveBufferSize();
        tcpNoDelay = cfg.isTcpNoDelay();
        threadPoolSize = cfg.getThreadPoolSize();
        idleTimeout = cfg.getIdleTimeout();
        jdbcEnabled = cfg.isJdbcEnabled();
        odbcEnabled = cfg.isOdbcEnabled();
        thinCliEnabled = cfg.isThinClientEnabled();
        sslEnabled = cfg.isSslEnabled();
        useIgniteSslCtxFactory = cfg.isUseIgniteSslContextFactory();
        sslClientAuth = cfg.isSslClientAuth();
        sslCtxFactory = compactClass(cfg.getSslContextFactory());
    }

    /**
     * @return Host.
     */
    @Nullable public String getHost() {
        return host;
    }

    /**
     * @return Port.
     */
    public int getPort() {
        return port;
    }

    /**
     * @return Port range.
     */
    public int getPortRange() {
        return portRange;
    }

    /**
     * @return Maximum number of opened cursors.
     */
    public int getMaxOpenCursorsPerConnection() {
        return maxOpenCursorsPerConn;
    }

    /**
     * @return Socket send buffer size in bytes.
     */
    public int getSocketSendBufferSize() {
        return sockSndBufSize;
    }

    /**
     * @return Socket receive buffer size in bytes.
     */
    public int getSocketReceiveBufferSize() {
        return sockRcvBufSize;
    }

    /**
     * @return TCP NO_DELAY flag.
     */
    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * @return Thread pool that is in charge of processing SQL requests.
     */
    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    /**
     * @return Idle timeout.
     */
    public long getIdleTimeout() {
        return idleTimeout;
    }

    /**
     * @return JDBC connections enabled flag.
     */
    public boolean isJdbcEnabled() {
        return jdbcEnabled;
    }

    /**
     * @return ODBC connections enabled flag.
     */
    public boolean isOdbcEnabled() {
        return odbcEnabled;
    }

    /**
     * @return JDBC connections enabled flag.
     */
    public boolean isThinClientEnabled() {
        return thinCliEnabled;
    }

    /**
     * @return SSL enable flag, default is disabled.
     */
    public boolean isSslEnabled() {
        return sslEnabled;
    }

    /**
     * @return If to use SSL context factory from Ignite configuration.
     */
    public boolean isUseIgniteSslContextFactory() {
        return useIgniteSslCtxFactory;
    }

    /**
     * @return SSL need client auth flag.
     */
    public boolean isSslClientAuth() {
        return sslClientAuth;
    }

    /**
     * @return SSL connection factory.
     */
    public String getSslContextFactory() {
        return sslCtxFactory;
    }

    @Override public byte getProtocolVersion() {
        return V2;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, host);
        out.writeInt(port);
        out.writeInt(portRange);
        out.writeInt(maxOpenCursorsPerConn);
        out.writeInt(sockSndBufSize);
        out.writeInt(sockRcvBufSize);
        out.writeBoolean(tcpNoDelay);
        out.writeInt(threadPoolSize);
        out.writeLong(idleTimeout);
        out.writeBoolean(jdbcEnabled);
        out.writeBoolean(odbcEnabled);
        out.writeBoolean(thinCliEnabled);
        out.writeBoolean(sslEnabled);
        out.writeBoolean(useIgniteSslCtxFactory);
        out.writeBoolean(sslClientAuth);
        U.writeString(out, sslCtxFactory);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        host = U.readString(in);
        port = in.readInt();
        portRange = in.readInt();
        maxOpenCursorsPerConn = in.readInt();
        sockSndBufSize = in.readInt();
        sockRcvBufSize = in.readInt();
        tcpNoDelay = in.readBoolean();
        threadPoolSize = in.readInt();

        if (protoVer > V1) {
            idleTimeout = in.readLong();
            jdbcEnabled = in.readBoolean();
            odbcEnabled = in.readBoolean();
            thinCliEnabled = in.readBoolean();
            sslEnabled = in.readBoolean();
            useIgniteSslCtxFactory = in.readBoolean();
            sslClientAuth = in.readBoolean();
            sslCtxFactory = U.readString(in);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorClientConnectorConfiguration.class, this);
    }
}
