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

package org.apache.ignite.internal.client.thin;

import java.net.InetSocketAddress;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.client.SslProtocol;

/**
 * Configuration required to initialize {@link TcpClientChannel}.
 */
final class ClientChannelConfiguration {
    /** Host. */
    private InetSocketAddress addr;

    /** Ssl mode. */
    private SslMode sslMode;

    /** Tcp no delay. */
    private boolean tcpNoDelay;

    /** Timeout. */
    private int timeout;

    /** Send buffer size. */
    private int sndBufSize;

    /** Receive buffer size. */
    private int rcvBufSize;

    /** Ssl client certificate key store path. */
    private String sslClientCertKeyStorePath;

    /** Ssl client certificate key store type. */
    private String sslClientCertKeyStoreType;

    /** Ssl client certificate key store password. */
    private String sslClientCertKeyStorePwd;

    /** Ssl trust certificate key store path. */
    private String sslTrustCertKeyStorePath;

    /** Ssl trust certificate key store type. */
    private String sslTrustCertKeyStoreType;

    /** Ssl trust certificate key store password. */
    private String sslTrustCertKeyStorePwd;

    /** Ssl key algorithm. */
    private String sslKeyAlgorithm;

    /** Ssl protocol. */
    private SslProtocol sslProto;

    /** Ssl trust all. */
    private boolean sslTrustAll;

    /** SSL Context Factory. */
    private Factory<SSLContext> sslCtxFactory;

    /** User. */
    private String userName;

    /** Password. */
    private String userPwd;

    /**
     * Constructor.
     */
    ClientChannelConfiguration(ClientConfiguration cfg) {
        this.sslMode = cfg.getSslMode();
        this.tcpNoDelay = cfg.isTcpNoDelay();
        this.timeout = cfg.getTimeout();
        this.sndBufSize = cfg.getSendBufferSize();
        this.rcvBufSize = cfg.getReceiveBufferSize();
        this.sslClientCertKeyStorePath = cfg.getSslClientCertificateKeyStorePath();
        this.sslClientCertKeyStoreType = cfg.getSslClientCertificateKeyStoreType();
        this.sslClientCertKeyStorePwd = cfg.getSslClientCertificateKeyStorePassword();
        this.sslTrustCertKeyStorePath = cfg.getSslTrustCertificateKeyStorePath();
        this.sslTrustCertKeyStoreType = cfg.getSslTrustCertificateKeyStoreType();
        this.sslTrustCertKeyStorePwd = cfg.getSslTrustCertificateKeyStorePassword();
        this.sslKeyAlgorithm = cfg.getSslKeyAlgorithm();
        this.sslProto = cfg.getSslProtocol();
        this.sslTrustAll = cfg.isSslTrustAll();
        this.sslCtxFactory = cfg.getSslContextFactory();
        this.userName = cfg.getUserName();
        this.userPwd = cfg.getUserPassword();
    }

    /**
     * @return Address.
     */
    public InetSocketAddress getAddress() {
        return addr;
    }

    /**
     * @param newVal Address.
     */
    public ClientChannelConfiguration setAddress(InetSocketAddress newVal) {
        addr = newVal;

        return this;
    }

    /**
     * @return SSL Mode.
     */
    public SslMode getSslMode() {
        return sslMode;
    }

    /**
     * @return Tcp no delay.
     */
    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * @return Timeout.
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * @return Send buffer size.
     */
    public int getSendBufferSize() {
        return sndBufSize;
    }

    /**
     * @return Receive buffer size.
     */
    public int getReceiveBufferSize() {
        return rcvBufSize;
    }

    /**
     * @return Ssl client certificate key store path.
     */
    public String getSslClientCertificateKeyStorePath() {
        return sslClientCertKeyStorePath;
    }

    /**
     * @return Ssl client certificate key store type.
     */
    public String getSslClientCertificateKeyStoreType() {
        return sslClientCertKeyStoreType;
    }

    /**
     * @return Ssl client certificate key store password.
     */
    public String getSslClientCertificateKeyStorePassword() {
        return sslClientCertKeyStorePwd;
    }

    /**
     * @return Ssl trust certificate key store path.
     */
    public String getSslTrustCertificateKeyStorePath() {
        return sslTrustCertKeyStorePath;
    }

    /**
     * @return Ssl trust certificate key store type.
     */
    public String getSslTrustCertificateKeyStoreType() {
        return sslTrustCertKeyStoreType;
    }

    /**
     * @return Ssl trust certificate key store password.
     */
    public String getSslTrustCertificateKeyStorePassword() {
        return sslTrustCertKeyStorePwd;
    }

    /**
     * @return Ssl key algorithm.
     */
    public String getSslKeyAlgorithm() {
        return sslKeyAlgorithm;
    }

    /**
     * @return SSL Protocol.
     */
    public SslProtocol getSslProtocol() {
        return sslProto;
    }

    /**
     * @return SSL Trust All.
     */
    public boolean isSslTrustAll() {
        return sslTrustAll;
    }

    /**
     * @return SSL Context Factory.
     */
    public Factory<SSLContext> getSslContextFactory() {
        return sslCtxFactory;
    }

    /**
     * @return User.
     */
    public String getUserName() {
        return userName;
    }

    /**
     * @return Password.
     */
    public String getUserPassword() {
        return userPwd;
    }
}
