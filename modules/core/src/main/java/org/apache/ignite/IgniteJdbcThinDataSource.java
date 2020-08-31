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

package org.apache.ignite;

import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.jdbc.thin.ConnectionPropertiesImpl;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.util.HostAndPortRange;
import org.apache.ignite.internal.util.typedef.F;

/**
 * JDBC thin DataSource implementation.
 */
public class IgniteJdbcThinDataSource implements DataSource, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Connection properties. */
    private ConnectionPropertiesImpl props = new ConnectionPropertiesImpl();

    /** Login timeout. */
    private int loginTimeout;

    /** {@inheritDoc} */
    @Override public Connection getConnection() throws SQLException {
        return getConnection(null, null);
    }

    /** {@inheritDoc} */
    @Override public Connection getConnection(String username, String pwd) throws SQLException {
        Properties props = this.props.storeToProperties();

        if (!F.isEmpty(username))
            props.put("user", username);

        if (!F.isEmpty(pwd))
            props.put("password", pwd);

        return IgniteJdbcThinDriver.register().connect(getUrl(), props);
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(iface))
            throw new SQLException("DataSource is not a wrapper for " + iface.getName());

        return (T)this;
    }

    /** {@inheritDoc} */
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(IgniteJdbcThinDataSource.class);
    }

    /** {@inheritDoc} */
    @Override public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void setLogWriter(PrintWriter out) throws SQLException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void setLoginTimeout(int seconds) throws SQLException {
        loginTimeout = seconds;
    }

    /** {@inheritDoc} */
    @Override public int getLoginTimeout() throws SQLException {
        return loginTimeout;
    }

    /** {@inheritDoc} */
    @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return Logger.getLogger("org.apache.ignite");
    }

    /**
     * Different application servers us different format (URL & url).
     * @return Connection URL.
     */
    public String getURL() {
        return getUrl();
    }

    /**
     * Different application servers us different format (URL & url).
     * @param url Connection URL.
     * @throws SQLException On error whrn URL is invalid.
     */
    public void setURL(String url) throws SQLException {
        setUrl(url);
    }

    /**
     * @return Ignite nodes addresses.
     */
    public String[] getAddresses() {
        HostAndPortRange[] addrs = props.getAddresses();

        if (addrs == null)
            return null;

        String[] addrsStr = new String[addrs.length];

        for (int i = 0; i < addrs.length; ++i)
            addrsStr[i] = addrs[i].toString();

        return addrsStr;
    }

    /**
     * Sets the addresses of the Ignite nodes to connect;
     * address string format: {@code host[:portRangeFrom[..portRangeTo]]}.
     *
     * Examples:
     * <ul>
     *     <li>"127.0.0.1"</li>
     *     <li>"127.0.0.1:10800"</li>
     *     <li>"127.0.0.1:10800..10810"</li>
     *     <li>"mynode0.mydomain.org:10800..10810", "mynode1.mydomain.org:10800..10810", "127.0.0.1:10800"</li>
     * <ul/>
     *
     * @param addrsStr Ignite nodes addresses.
     * @throws SQLException On invalid addresses.
     */
    public void setAddresses(String... addrsStr) throws SQLException {
        HostAndPortRange[] addrs = new HostAndPortRange[addrsStr.length];

        for (int i = 0; i < addrs.length; ++i) {
            try {
                addrs[i] = HostAndPortRange.parse(addrsStr[i],
                    ClientConnectorConfiguration.DFLT_PORT, ClientConnectorConfiguration.DFLT_PORT,
                    "Invalid endpoint format (should be \"host[:portRangeFrom[..portRangeTo]]\")");
            }
            catch (IgniteCheckedException e) {
                throw new SQLException(e.getMessage(), SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
        }

        props.setAddresses(addrs);
    }

    /**
     * @return Schema name of the connection.
     */
    public String getSchema() {
        return props.getSchema();
    }

    /**
     * @param schema Schema name of the connection.
     */
    public void setSchema(String schema) {
        props.setSchema(schema);
    }

    /**
     * @return The URL of the connection.
     */
    public String getUrl() {
        return props.getUrl();
    }

    /**
     * @param url The URL of the connection.
     * @throws SQLException On invalid URL.
     */
    public void setUrl(String url) throws SQLException {
        props = new ConnectionPropertiesImpl();

        props.setUrl(url);
    }

    /**
     * @return Distributed joins flag.
     */
    public boolean isDistributedJoins() {
        return props.isDistributedJoins();
    }

    /**
     * @param distributedJoins Distributed joins flag.
     */
    public void setDistributedJoins(boolean distributedJoins) {
        props.setDistributedJoins(distributedJoins);
    }

    /**
     * @return Enforce join order flag.
     */
    public boolean isEnforceJoinOrder() {
        return props.isEnforceJoinOrder();
    }

    /**
     * @param enforceJoinOrder Enforce join order flag.
     */
    public void setEnforceJoinOrder(boolean enforceJoinOrder) {
        props.setEnforceJoinOrder(enforceJoinOrder);
    }

    /**
     * @return Collocated flag.
     */
    public boolean isCollocated() {
        return props.isCollocated();
    }

    /**
     * @param collocated Collocated flag.
     */
    public void setCollocated(boolean collocated) {
        props.setCollocated(collocated);
    }

    /**
     * @return Replicated only flag.
     */
    public boolean isReplicatedOnly() {
        return props.isReplicatedOnly();
    }

    /**
     * @param replicatedOnly Replicated only flag.
     */
    public void setReplicatedOnly(boolean replicatedOnly) {
        props.setReplicatedOnly(replicatedOnly);
    }

    /**
     * @return Auto close server cursors flag.
     */
    public boolean isAutoCloseServerCursor() {
        return props.isAutoCloseServerCursor();
    }

    /**
     * @param autoCloseServerCursor Auto close server cursors flag.
     */
    public void setAutoCloseServerCursor(boolean autoCloseServerCursor) {
        props.setAutoCloseServerCursor(autoCloseServerCursor);
    }

    /**
     * @return Socket send buffer size.
     */
    public int getSocketSendBuffer() {
        return props.getSocketSendBuffer();
    }

    /**
     * @param size Socket send buffer size.
     * @throws SQLException On error.
     */
    public void setSocketSendBuffer(int size) throws SQLException {
        props.setSocketSendBuffer(size);
    }

    /**
     * @return Socket receive buffer size.
     */
    public int getSocketReceiveBuffer() {
        return props.getSocketReceiveBuffer();
    }

    /**
     * @param size Socket receive buffer size.
     * @throws SQLException On error.
     */
    public void setSocketReceiveBuffer(int size) throws SQLException {
        props.setSocketReceiveBuffer(size);
    }

    /**
     * @return TCP no delay flag.
     */
    public boolean isTcpNoDelay() {
        return props.isTcpNoDelay();
    }

    /**
     * @param tcpNoDelay TCP no delay flag.
     */
    public void setTcpNoDelay(boolean tcpNoDelay) {
        props.setTcpNoDelay(tcpNoDelay);
    }

    /**
     * @return Lazy query execution flag.
     */
    public boolean isLazy() {
        return props.isLazy();
    }

    /**
     * @param lazy Lazy query execution flag.
     */
    public void setLazy(boolean lazy) {
        props.setLazy(lazy);
    }

    /**
     * @return Skip reducer on update flag.
     */
    public boolean isSkipReducerOnUpdate() {
        return props.isSkipReducerOnUpdate();
    }

    /**
     * @param skipReducerOnUpdate Skip reducer on update flag.
     */
    public void setSkipReducerOnUpdate(boolean skipReducerOnUpdate) {
        props.setSkipReducerOnUpdate(skipReducerOnUpdate);
    }

    /**
     * Gets SSL connection mode.
     *
     * @return Use SSL flag.
     * @see #setSslMode(String).
     */
    public String getSslMode() {
        return props.getSslMode();
    }

    /**
     * Use SSL connection to Ignite node. In case set to {@code "require"} SSL context must be configured.
     * {@link #setSslClientCertificateKeyStoreUrl} property and related properties must be set up
     * or JSSE properties must be set up (see {@code javax.net.ssl.keyStore} and other {@code javax.net.ssl.*}
     * properties)
     *
     * In case set to {@code "disable"} plain connection is used.
     * Available modes: {@code "disable", "require"}. Default value is {@code "disable"}
     *
     * @param mode SSL mode.
     */
    public void setSslMode(String mode) {
        props.setSslMode(mode);
    }

    /**
     * Gets protocol for secure transport.
     *
     * @return SSL protocol name.
     */
    public String getSslProtocol() {
        return props.getSslProtocol();
    }

    /**
     * Sets protocol for secure transport. If not specified, TLS protocol will be used.
     * Protocols implementations supplied by JSEE: SSLv3 (SSL), TLSv1 (TLS), TLSv1.1, TLSv1.2
     *
     * <p>See more at JSSE Reference Guide.
     *
     * @param sslProtocol SSL protocol name.
     */
    public void setSslProtocol(String sslProtocol) {
        props.setSslProtocol(sslProtocol);
    }

    /**
     * Gets cipher suites.
     *
     * @return SSL cipher suites.
     */
    public String getCipherSuites() {
        return props.getSslCipherSuites();
    }

    /**
     * Override default cipher suites.
     *
     * <p>See more at JSSE Reference Guide.
     *
     * @param cipherSuites SSL cipher suites.
     */
    public void setCipherSuites(String cipherSuites) {
        props.setSslCipherSuites(cipherSuites);
    }

    /**
     * Gets algorithm that will be used to create a key manager.
     *
     * @return Key manager algorithm.
     */
    public String getSslKeyAlgorithm() {
        return props.getSslKeyAlgorithm();
    }

    /**
     * Sets key manager algorithm that will be used to create a key manager. Notice that in most cased default value
     * suites well, however, on Android platform this value need to be set to <tt>X509<tt/>.
     * Algorithms implementations supplied by JSEE: PKIX (X509 or SunPKIX), SunX509
     *
     * <p>See more at JSSE Reference Guide.
     *
     * @param keyAlgorithm Key algorithm name.
     */
    public void setSslKeyAlgorithm(String keyAlgorithm) {
        props.setSslKeyAlgorithm(keyAlgorithm);
    }

    /**
     * Gets the key store URL.
     *
     * @return Client certificate KeyStore URL.
     */
    public String getSslClientCertificateKeyStoreUrl() {
        return props.getSslClientCertificateKeyStoreUrl();
    }

    /**
     * Sets path to the key store file. This is a mandatory parameter since
     * ssl context could not be initialized without key manager.
     *
     * In case {@link #getSslMode()} is {@code required} and key store URL isn't specified by Ignite properties
     * (e.g. at JDBC URL) the JSSE property {@code javax.net.ssl.keyStore} will be used.
     *
     * @param url Client certificate KeyStore URL.
     */
    public void setSslClientCertificateKeyStoreUrl(String url) {
        props.setSslClientCertificateKeyStoreUrl(url);
    }

    /**
     * Gets key store password.
     *
     * @return Client certificate KeyStore password.
     */
    public String getSslClientCertificateKeyStorePassword() {
        return props.getSslClientCertificateKeyStorePassword();
    }

    /**
     * Sets key store password.
     *
     * In case {@link #getSslMode()} is {@code required}  and key store password isn't specified by Ignite properties
     * (e.g. at JDBC URL) the JSSE property {@code javax.net.ssl.keyStorePassword} will be used.
     *
     * @param passwd Client certificate KeyStore password.
     */
    public void setSslClientCertificateKeyStorePassword(String passwd) {
        props.setSslClientCertificateKeyStorePassword(passwd);
    }

    /**
     * Gets key store type used for context creation.
     *
     * @return Client certificate KeyStore type.
     */
    public String getSslClientCertificateKeyStoreType() {
        return props.getSslClientCertificateKeyStoreType();
    }

    /**
     * Sets key store type used in context initialization.
     *
     * In case {@link #getSslMode()} is {@code required} and key store type isn't specified by Ignite properties
     *  (e.g. at JDBC URL)the JSSE property {@code javax.net.ssl.keyStoreType} will be used.
     * In case both Ignite properties and JSSE properties are not set the default 'JKS' type is used.
     *
     * <p>See more at JSSE Reference Guide.
     *
     * @param ksType Client certificate KeyStore type.
     */
    public void setSslClientCertificateKeyStoreType(String ksType) {
        props.setSslClientCertificateKeyStoreType(ksType);
    }

    /**
     * Gets the trust store URL.
     *
     * @return Trusted certificate KeyStore URL.
     */
    public String getSslTrustCertificateKeyStoreUrl() {
        return props.getSslTrustCertificateKeyStoreUrl();
    }

    /**
     * Sets path to the trust store file. This is an optional parameter,
     * however one of the {@code setSslTrustCertificateKeyStoreUrl(String)}, {@link #setSslTrustAll(boolean)}
     * properties must be set.
     *
     * In case {@link #getSslMode()} is {@code required} and trust store URL isn't specified by Ignite properties
     * (e.g. at JDBC URL) the JSSE property {@code javax.net.ssl.trustStore} will be used.
     *
     * @param url Trusted certificate KeyStore URL.
     */
    public void setSslTrustCertificateKeyStoreUrl(String url) {
        props.setSslTrustCertificateKeyStoreUrl(url);
    }

    /**
     * Gets trust store password.
     *
     * @return Trusted certificate KeyStore password.
     */
    public String getSslTrustCertificateKeyStorePassword() {
        return props.getSslTrustCertificateKeyStorePassword();
    }

    /**
     * Sets trust store password.
     *
     * In case {@link #getSslMode()} is {@code required} and trust store password isn't specified by Ignite properties
     * (e.g. at JDBC URL) the JSSE property {@code javax.net.ssl.trustStorePassword} will be used.
     *
     * @param passwd Trusted certificate KeyStore password.
     */
    public void setSslTrustCertificateKeyStorePassword(String passwd) {
        props.setSslTrustCertificateKeyStorePassword(passwd);
    }

    /**
     * Gets trust store type.
     *
     * @return Trusted certificate KeyStore type.
     */
    public String getSslTrustCertificateKeyStoreType() {
        return props.getSslTrustCertificateKeyStoreType();
    }

    /**
     * Sets trust store type.
     *
     * In case {@link #getSslMode()} is {@code required} and trust store type isn't specified by Ignite properties
     * (e.g. at JDBC URL) the JSSE property {@code javax.net.ssl.trustStoreType} will be used.
     * In case both Ignite properties and JSSE properties are not set the default 'JKS' type is used.
     *
     * @param ksType Trusted certificate KeyStore type.
     */
    public void setSslTrustCertificateKeyStoreType(String ksType) {
        props.setSslTrustCertificateKeyStoreType(ksType);
    }

    /**
     * Gets trust any server certificate flag.
     *
     * @return Trust all certificates flag.
     */
    public boolean isSslTrustAll() {
        return props.isSslTrustAll();
    }

    /**
     * Sets to {@code true} to trust any server certificate (revoked, expired or self-signed SSL certificates).
     *
     * <p> Defaults is {@code false}.
     *
     * Note: Do not enable this option in production you are ever going to use
     * on a network you do not entirely trust. Especially anything going over the public internet.
     *
     * @param trustAll Trust all certificates flag.
     */
    public void setSslTrustAll(boolean trustAll) {
        props.setSslTrustAll(trustAll);
    }

    /**
     * Gets the class name of the custom implementation of the Factory&lt;SSLSocketFactory&gt;.
     *
     * @return Custom class name that implements Factory&lt;SSLSocketFactory&gt;.
     */
    public String getSslFactory() {
        return props.getSslFactory();
    }

    /**
     * Sets the class name of the custom implementation of the Factory&lt;SSLSocketFactory&gt;.
     * If {@link #getSslMode()} is {@code required} and factory is specified the custom factory will be used
     * instead of JSSE socket factory. So, other SSL properties will be ignored.
     *
     * @param sslFactory Custom class name that implements Factory&lt;SSLSocketFactory&gt;.
     */
    public void setSslFactory(String sslFactory) {
        props.setSslFactory(sslFactory);
    }

    /**
     * @param name User name to authentication.
     */
    public void setUsername(String name) {
        props.setUsername(name);
    }

    /**
     * @return User name to authentication.
     */
    public String getUsername() {
        return props.getUsername();
    }

    /**
     * @param passwd User's password.
     */
    public void setPassword(String passwd) {
        props.setPassword(passwd);
    }

    /**
     * @return User's password.
     */
    public String getPassword() {
        return props.getPassword();
    }
}
