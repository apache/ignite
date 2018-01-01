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
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.jetbrains.annotations.Nullable;

import static java.lang.System.getProperty;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JETTY_HOST;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JETTY_PORT;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.intValue;

/**
 * Create data transfer object for node REST configuration properties.
 */
public class VisorRestConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Whether REST enabled or not. */
    private boolean restEnabled;

    /** Whether or not SSL is enabled for TCP binary protocol. */
    private boolean tcpSslEnabled;

    /** Jetty config path. */
    private String jettyPath;

    /** Jetty host. */
    private String jettyHost;

    /** Jetty port. */
    private Integer jettyPort;

    /** REST TCP binary host. */
    private String tcpHost;

    /** REST TCP binary port. */
    private int tcpPort;

    /** Context factory for SSL. */
    private String tcpSslCtxFactory;

    /** REST secret key. */
    private String secretKey;

    /** TCP no delay flag. */
    private boolean noDelay;

    /** REST TCP direct buffer flag. */
    private boolean directBuf;

    /** REST TCP send buffer size. */
    private int sndBufSize;

    /** REST TCP receive buffer size. */
    private int rcvBufSize;

    /** REST idle timeout for query cursor. */
    private long idleQryCurTimeout;

    /** REST idle check frequency for query cursor. */
    private long idleQryCurCheckFreq;

    /** REST TCP send queue limit. */
    private int sndQueueLimit;

    /** REST TCP selector count. */
    private int selectorCnt;

    /** Idle timeout. */
    private long idleTimeout;

    /** SSL need client auth flag. */
    private boolean sslClientAuth;

    /** SSL context factory for rest binary server. */
    private String sslFactory;

    /** Port range */
    private int portRange;

    /** Client message interceptor. */
    private String msgInterceptor;

    /**
     * Default constructor.
     */
    public VisorRestConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for node REST configuration properties.
     *
     * @param c Grid configuration.
     */
    public VisorRestConfiguration(IgniteConfiguration c) {
        assert c != null;

        ConnectorConfiguration conCfg = c.getConnectorConfiguration();

        restEnabled = conCfg != null;

        if (restEnabled) {
            tcpSslEnabled = conCfg.isSslEnabled();
            jettyPath = conCfg.getJettyPath();
            jettyHost = getProperty(IGNITE_JETTY_HOST);
            jettyPort = intValue(IGNITE_JETTY_PORT, null);
            tcpHost = conCfg.getHost();
            tcpPort = conCfg.getPort();
            tcpSslCtxFactory = compactClass(conCfg.getSslContextFactory());
            secretKey = conCfg.getSecretKey();
            noDelay = conCfg.isNoDelay();
            directBuf = conCfg.isDirectBuffer();
            sndBufSize = conCfg.getSendBufferSize();
            rcvBufSize = conCfg.getReceiveBufferSize();
            idleQryCurTimeout = conCfg.getIdleQueryCursorTimeout();
            idleQryCurCheckFreq = conCfg.getIdleQueryCursorCheckFrequency();
            sndQueueLimit = conCfg.getSendQueueLimit();
            selectorCnt = conCfg.getSelectorCount();
            idleTimeout = conCfg.getIdleTimeout();
            sslClientAuth = conCfg.isSslClientAuth();
            sslFactory = compactClass(conCfg.getSslFactory());
            portRange = conCfg.getPortRange();
            msgInterceptor = compactClass(conCfg.getMessageInterceptor());
        }
    }

    /**
     * @return Whether REST enabled or not.
     */
    public boolean isRestEnabled() {
        return restEnabled;
    }

    /**
     * @return Whether or not SSL is enabled for TCP binary protocol.
     */
    public boolean isTcpSslEnabled() {
        return tcpSslEnabled;
    }

    /**
     * @return Jetty config path.
     */
    @Nullable public String getJettyPath() {
        return jettyPath;
    }

    /**
     * @return Jetty host.
     */
    @Nullable public String getJettyHost() {
        return jettyHost;
    }

    /**
     * @return Jetty port.
     */
    @Nullable public Integer getJettyPort() {
        return jettyPort;
    }

    /**
     * @return REST TCP binary host.
     */
    @Nullable public String getTcpHost() {
        return tcpHost;
    }

    /**
     * @return REST TCP binary port.
     */
    public int getTcpPort() {
        return tcpPort;
    }

    /**
     * @return Context factory for SSL.
     */
    @Nullable public String getTcpSslContextFactory() {
        return tcpSslCtxFactory;
    }

    /**
     * @return Secret key.
     */
    @Nullable public String getSecretKey() {
        return secretKey;
    }

    /**
     * @return Whether {@code TCP_NODELAY} option should be enabled.
     */
    public boolean isNoDelay() {
        return noDelay;
    }

    /**
     * @return Whether direct buffer should be used.
     */
    public boolean isDirectBuffer() {
        return directBuf;
    }

    /**
     * @return REST TCP server send buffer size (0 for default).
     */
    public int getSendBufferSize() {
        return sndBufSize;
    }

    /**
     * @return REST TCP server receive buffer size (0 for default).
     */
    public int getReceiveBufferSize() {
        return rcvBufSize;
    }

    /**
     * @return Idle query cursors timeout in milliseconds
     */
    public long getIdleQueryCursorTimeout() {
        return idleQryCurTimeout;
    }

    /**
     * @return Idle query cursor check frequency in milliseconds.
     */
    public long getIdleQueryCursorCheckFrequency() {
        return idleQryCurCheckFreq;
    }

    /**
     * @return REST TCP server send queue limit (0 for unlimited).
     */
    public int getSendQueueLimit() {
        return sndQueueLimit;
    }

    /**
     * @return Number of selector threads for REST TCP server.
     */
    public int getSelectorCount() {
        return selectorCnt;
    }

    /**
     * @return Idle timeout in milliseconds.
     */
    public long getIdleTimeout() {
        return idleTimeout;
    }

    /**
     * Gets a flag indicating whether or not remote clients will be required to have a valid SSL certificate which
     * validity will be verified with trust manager.
     *
     * @return Whether or not client authentication is required.
     */
    public boolean isSslClientAuth() {
        return sslClientAuth;
    }

    /**
     *  @return SslContextFactory instance.
     */
    public String getSslFactory() {
        return sslFactory;
    }

    /**
     * @return Number of ports to try.
     */
    public int getPortRange() {
        return portRange;
    }

    /**
     * @return Interceptor.
     */
    @Nullable public String getMessageInterceptor() {
        return msgInterceptor;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(restEnabled);
        out.writeBoolean(tcpSslEnabled);
        U.writeString(out, jettyPath);
        U.writeString(out, jettyHost);
        out.writeObject(jettyPort);
        U.writeString(out, tcpHost);
        out.writeInt(tcpPort);
        U.writeString(out, tcpSslCtxFactory);
        U.writeString(out, secretKey);
        out.writeBoolean(noDelay);
        out.writeBoolean(directBuf);
        out.writeInt(sndBufSize);
        out.writeInt(rcvBufSize);
        out.writeLong(idleQryCurTimeout);
        out.writeLong(idleQryCurCheckFreq);
        out.writeInt(sndQueueLimit);
        out.writeInt(selectorCnt);
        out.writeLong(idleTimeout);
        out.writeBoolean(sslClientAuth);
        U.writeString(out, sslFactory);
        out.writeInt(portRange);
        U.writeString(out, msgInterceptor);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        restEnabled = in.readBoolean();
        tcpSslEnabled = in.readBoolean();
        jettyPath = U.readString(in);
        jettyHost = U.readString(in);
        jettyPort = (Integer)in.readObject();
        tcpHost = U.readString(in);
        tcpPort = in.readInt();
        tcpSslCtxFactory = U.readString(in);
        secretKey = U.readString(in);
        noDelay = in.readBoolean();
        directBuf = in.readBoolean();
        sndBufSize = in.readInt();
        rcvBufSize = in.readInt();
        idleQryCurTimeout = in.readLong();
        idleQryCurCheckFreq = in.readLong();
        sndQueueLimit = in.readInt();
        selectorCnt = in.readInt();
        idleTimeout = in.readLong();
        sslClientAuth = in.readBoolean();
        sslFactory = U.readString(in);
        portRange = in.readInt();
        msgInterceptor = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorRestConfiguration.class, this);
    }
}
