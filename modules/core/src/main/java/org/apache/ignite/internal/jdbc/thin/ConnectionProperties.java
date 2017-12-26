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

package org.apache.ignite.internal.jdbc.thin;

import java.sql.SQLException;

/**
 * Provide access and manipulations with connection JDBC properties.
 */
public interface ConnectionProperties {
    /**
     * @return Host name or host's IP to connect.
     */
    public String getHost();

    /**
     * @param host Host name or host's IP to connect.
     */
    public void setHost(String host);

    /**
     * @return Port to connect.
     */
    public int getPort();

    /**
     * @param port Port to connect.
     * @throws SQLException On error.
     */
    public void setPort(int port) throws SQLException;

    /**
     * @return Distributed joins flag.
     */
    public boolean isDistributedJoins();

    /**
     * @param distributedJoins Distributed joins flag.
     */
    public void setDistributedJoins(boolean distributedJoins);

    /**
     * @return Enforce join order flag.
     */
    public boolean isEnforceJoinOrder();

    /**
     * @param enforceJoinOrder Enforce join order flag.
     */
    public void setEnforceJoinOrder(boolean enforceJoinOrder);

    /**
     * @return Collocated flag.
     */
    public boolean isCollocated();

    /**
     * @param collocated Collocated flag.
     */
    public void setCollocated(boolean collocated);

    /**
     * @return Replicated only flag.
     */
    public boolean isReplicatedOnly();

    /**
     * @param replicatedOnly Replicated only flag.
     */
    public void setReplicatedOnly(boolean replicatedOnly);

    /**
     * @return Auto close server cursors flag.
     */
    public boolean isAutoCloseServerCursor();

    /**
     * @param autoCloseServerCursor Auto close server cursors flag.
     */
    public void setAutoCloseServerCursor(boolean autoCloseServerCursor);

    /**
     * @return Socket send buffer size.
     */
    public int getSocketSendBuffer();

    /**
     * @param size Socket send buffer size.
     * @throws SQLException On error.
     */
    public void setSocketSendBuffer(int size) throws SQLException;

    /**
     * @return Socket receive buffer size.
     */
    public int getSocketReceiveBuffer();

    /**
     * @param size Socket receive buffer size.
     * @throws SQLException On error.
     */
    public void setSocketReceiveBuffer(int size) throws SQLException;

    /**
     * @return TCP no delay flag.
     */
    public boolean isTcpNoDelay();

    /**
     * @param tcpNoDelay TCP no delay flag.
     */
    public void setTcpNoDelay(boolean tcpNoDelay);

    /**
     * @return Lazy query execution flag.
     */
    public boolean isLazy();

    /**
     * @param lazy Lazy query execution flag.
     */
    public void setLazy(boolean lazy);

    /**
     * @return Skip reducer on update flag.
     */
    public boolean isSkipReducerOnUpdate();

    /**
     * @param skipReducerOnUpdate Skip reducer on update flag.
     */
    public void setSkipReducerOnUpdate(boolean skipReducerOnUpdate);

    /**
     * @return Use SSL flag.
     */
    public boolean isUseSSL();

    /**
     * @param useSSL Use SSL flag.
     */
    public void setUseSSL(boolean useSSL);

    /**
     * @return SSL protocol name.
     */
    public String sslProtocol();

    /**
     * @param sslProtocol SSL protocol name.
     */
    public void setSslProtocol(String sslProtocol);

    /**
     * @return Client certificate KeyStore URL.
     */
    public String getSslClientCertificateKeyStoreUrl();

    /**
     * @param url Client certificate KeyStore URL.
     */
    public void setSslClientCertificateKeyStoreUrl(String url);

    /**
     * @return Client certificate KeyStore password.
     */
    public String getSslClientCertificateKeyStorePassword();

    /**
     * @param passwd Client certificate KeyStore password.
     */
    public void setSslClientCertificateKeyStorePassword(String passwd);

    /**
     * @return Client certificate KeyStore type.
     */
    public String getSslClientCertificateKeyStoreType();

    /**
     * @param ksType Client certificate KeyStore type.
     */
    public void setSslClientCertificateKeyStoreType(String ksType);

    /**
     * @return Trusted certificate KeyStore URL.
     */
    public String getSslTrustCertificateKeyStoreUrl();

    /**
     * @param url Trusted certificate KeyStore URL.
     */
    public void setSslTrustCertificateKeyStoreUrl(String url);

    /**
     * @return Trusted certificate KeyStore password.
     */
    public String getSslTrustCertificateKeyStorePassword();

    /**
     * @param passwd Trusted certificate KeyStore password.
     */
    public void setSslTrustCertificateKeyStorePassword(String passwd);

    /**
     * @return Trusted certificate KeyStore type.
     */
    public String getSslTrustCertificateKeyStoreType();

    /**
     * @param ksType Trusted certificate KeyStore type.
     */
    public void setSslTrustCertificateKeyStoreType(String ksType);

    /**
     * @return Trust all certificates flag.
     */
    public boolean isSslTrustAll();

    /**
     * @param trustAll Trust all certificates flag.
     */
    public void setSslTrustAll(boolean trustAll);

    /**
     * @return Custom class name that implements Factory&lt;SSLSocketFactory&gt;.
     */
    public String getSslFactory();

    /**
     * @param sslFactory Custom class name that implements Factory&lt;SSLSocketFactory&gt;.
     */
    public void setSslFactory(String sslFactory);

    /**
     * @return Use default SSL context flag.
     */
    public boolean isSslUseDefault();

    /**
     * @param useDefault Use default SSL context flag.
     */
    public void setSslUseDefault(boolean useDefault);
}
