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

import java.io.Serializable;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import java.util.StringTokenizer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcThinFeature;
import org.apache.ignite.internal.processors.query.NestedTxMode;
import org.apache.ignite.internal.util.HostAndPortRange;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Holds JDBC connection properties.
 */
public class ConnectionPropertiesImpl implements ConnectionProperties, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Prefix for property names. */
    public static final String PROP_PREFIX = "ignite.jdbc.";

    /** Default socket buffer size. */
    private static final int DFLT_SOCK_BUFFER_SIZE = 64 * 1024;

    /** Property: schema. */
    private static final String PROP_SCHEMA = "schema";

    /** Connection URL. */
    private String url;

    /** Addresses. */
    private HostAndPortRange[] addrs;

    /** Schema name. Hidden property. Is used to set default schema name part of the URL. */
    private StringProperty schema = new StringProperty(PROP_SCHEMA,
        "Schema name of the connection", "PUBLIC", null, false, null);

    /** Distributed joins property. */
    private BooleanProperty distributedJoins = new BooleanProperty(
        "distributedJoins", "Enable distributed joins", false, false);

    /** Enforce join order property. */
    private BooleanProperty enforceJoinOrder = new BooleanProperty(
        "enforceJoinOrder", "Enable enforce join order", false, false);

    /** Collocated property. */
    private BooleanProperty collocated = new BooleanProperty(
        "collocated", "Enable collocated query", false, false);

    /** Replicated only property. */
    private BooleanProperty replicatedOnly = new BooleanProperty(
        "replicatedOnly", "Specify if the all queries contain only replicated tables", false, false);

    /** Auto close server cursor property. */
    private BooleanProperty autoCloseServerCursor = new BooleanProperty(
        "autoCloseServerCursor", "Enable auto close server cursors when last piece of result set is retrieved. " +
        "If the server-side cursor is already closed, you may get an exception when trying to call " +
        "`ResultSet.getMetadata()` method.", false, false);

    /** TCP no delay property. */
    private BooleanProperty tcpNoDelay = new BooleanProperty(
        "tcpNoDelay", "TCP no delay flag", true, false);

    /** Lazy query execution property. */
    private BooleanProperty lazy = new BooleanProperty(
        "lazy", "Enable lazy query execution", false, false);

    /** Socket send buffer size property. */
    private IntegerProperty socketSendBuffer = new IntegerProperty(
        "socketSendBuffer", "Socket send buffer size",
        DFLT_SOCK_BUFFER_SIZE, false, 0, Integer.MAX_VALUE);

    /** Socket receive buffer size property. */
    private IntegerProperty socketReceiveBuffer = new IntegerProperty(
        "socketReceiveBuffer", "Socket send buffer size",
        DFLT_SOCK_BUFFER_SIZE, false, 0, Integer.MAX_VALUE);

    /** Executes update queries on ignite server nodes flag. */
    private BooleanProperty skipReducerOnUpdate = new BooleanProperty(
        "skipReducerOnUpdate", "Enable execution update queries on ignite server nodes", false, false);

    /** Nested transactions handling strategy. */
    private StringProperty nestedTxMode = new StringProperty(
        "nestedTransactionsMode", "Way to handle nested transactions", NestedTxMode.ERROR.name(),
        new String[] { NestedTxMode.COMMIT.name(), NestedTxMode.ERROR.name(), NestedTxMode.IGNORE.name() },
        false, new PropertyValidator() {
        private static final long serialVersionUID = 0L;

        @Override public void validate(String mode) throws SQLException {
            if (!F.isEmpty(mode)) {
                try {
                    NestedTxMode.valueOf(mode.toUpperCase());
                }
                catch (IllegalArgumentException e) {
                    throw new SQLException("Invalid nested transactions handling mode, allowed values: " +
                        Arrays.toString(nestedTxMode.choices), SqlStateCode.CLIENT_CONNECTION_FAILED);
                }
            }
        }
    });

    /** SSL: Use SSL connection to Ignite node. */
    private StringProperty sslMode = new StringProperty("sslMode",
        "The SSL mode of the connection", SSL_MODE_DISABLE,
        new String[] {SSL_MODE_DISABLE, SSL_MODE_REQUIRE}, false, null);

    /** SSL: Client certificate key store url. */
    private StringProperty sslProtocol = new StringProperty("sslProtocol",
        "SSL protocol name", null, null, false, null);

    /** SSL: Supported SSL cipher suites. */
    private StringProperty sslCipherSuites = new StringProperty("sslCipherSuites",
        "Supported SSL ciphers", null,
        null, false, null);

    /** SSL: Key algorithm name. */
    private StringProperty sslKeyAlgorithm = new StringProperty("sslKeyAlgorithm",
        "SSL key algorithm name", "SunX509", null, false, null);

    /** SSL: Client certificate key store url. */
    private StringProperty sslClientCertificateKeyStoreUrl =
        new StringProperty("sslClientCertificateKeyStoreUrl",
            "Client certificate key store URL",
            null, null, false, null);

    /** SSL: Client certificate key store password. */
    private StringProperty sslClientCertificateKeyStorePassword =
        new StringProperty("sslClientCertificateKeyStorePassword",
            "Client certificate key store password",
            null, null, false, null);

    /** SSL: Client certificate key store type. */
    private StringProperty sslClientCertificateKeyStoreType =
        new StringProperty("sslClientCertificateKeyStoreType",
            "Client certificate key store type",
            null, null, false, null);

    /** SSL: Trusted certificate key store url. */
    private StringProperty sslTrustCertificateKeyStoreUrl =
        new StringProperty("sslTrustCertificateKeyStoreUrl",
            "Trusted certificate key store URL", null, null, false, null);

    /** SSL Trusted certificate key store password. */
    private StringProperty sslTrustCertificateKeyStorePassword =
        new StringProperty("sslTrustCertificateKeyStorePassword",
            "Trusted certificate key store password", null, null, false, null);

    /** SSL: Trusted certificate key store type. */
    private StringProperty sslTrustCertificateKeyStoreType =
        new StringProperty("sslTrustCertificateKeyStoreType",
            "Trusted certificate key store type",
            null, null, false, null);

    /** SSL: Trust all certificates. */
    private BooleanProperty sslTrustAll = new BooleanProperty("sslTrustAll",
        "Trust all certificates", false, false);

    /** SSL: Custom class name that implements Factory&lt;SSLSocketFactory&gt;. */
    private StringProperty sslFactory = new StringProperty("sslFactory",
        "Custom class name that implements Factory<SSLSocketFactory>", null, null, false, null);

    /** Custom class name that implements Factory&lt;Map&lt;String, String&gt;&gt; which returns user attributes. */
    private StringProperty userAttrsFactory = new StringProperty("userAttributesFactory",
        "Custom class name that implements Factory<Map<String, String>> (user attributes)", null, null, false, null);

    /** User name to authenticate the client on the server side. */
    private StringProperty user = new StringProperty(
        "user", "User name to authenticate the client on the server side", null, null, false, null);

    /** User's password. */
    private StringProperty passwd = new StringProperty(
        "password", "User's password", null, null, false, null);

    /** Data page scan flag. */
    private BooleanProperty dataPageScanEnabled = new BooleanProperty("dataPageScanEnabled",
        "Whether data page scan for queries is allowed. If not specified, server defines the default behaviour.",
        null, false);

    /** Partition awareness flag. */
    private BooleanProperty partitionAwareness = new BooleanProperty(
        "partitionAwareness",
        "Whether jdbc thin partition awareness is enabled.",
        false, false);

    /** Update batch size (the size of internal batches are used for INSERT/UPDATE/DELETE operation). */
    private IntegerProperty updateBatchSize = new IntegerProperty("updateBatchSize",
        "Update bach size (the size of internal batches are used for INSERT/UPDATE/DELETE operation). " +
            "Set to 1 to prevent deadlock on update where keys sequence are different " +
            "in several concurrent updates.", null, false, 1, Integer.MAX_VALUE);

    /** Partition awareness SQL cache size. */
    private IntegerProperty partitionAwarenessSQLCacheSize = new IntegerProperty("partitionAwarenessSQLCacheSize",
        "The size of sql cache that is used within partition awareness optimization.",
        1_000, false, 1, Integer.MAX_VALUE);

    /** Partition awareness partition distributions cache size. */
    private IntegerProperty partitionAwarenessPartDistributionsCacheSize = new IntegerProperty(
        "partitionAwarenessPartitionDistributionsCacheSize",
        "The size of partition distributions cache that is used within partition awareness optimization.",
        1_000, false, 1, Integer.MAX_VALUE);

    /** Query timeout. */
    private IntegerProperty qryTimeout = new IntegerProperty("queryTimeout",
        "Sets the number of seconds the driver will wait for a <code>Statement</code> object to execute." +
            " Zero means there is no limits.",
        0, false, 0, Integer.MAX_VALUE);

    /** JDBC connection timeout. */
    private IntegerProperty connTimeout = new IntegerProperty("connectionTimeout",
        "Sets the number of milliseconds JDBC client will waits for server to response." +
            " Zero means there is no limits.",
        0, false, 0, Integer.MAX_VALUE);

    /** Disabled features. */
    private StringProperty disabledFeatures = new StringProperty("disabledFeatures",
        "Sets enumeration of features to force disable its.", null, null, false, new PropertyValidator() {
        @Override public void validate(String val) throws SQLException {
            if (val == null)
                return;

            String[] features = val.split("\\W+");

            for (String f : features) {
                try {
                    JdbcThinFeature.valueOf(f.toUpperCase());
                }
                catch (IllegalArgumentException e) {
                    throw new SQLException("Unknown feature: " + f);
                }
            }
        }
    });

    /** Keep binary objects in binary form. */
    private BooleanProperty keepBinary = new BooleanProperty("keepBinary",
        "Whether to keep binary objects in binary form.", false, false);

    /** Properties array. */
    private final ConnectionProperty[] propsArray = {
        distributedJoins, enforceJoinOrder, collocated, replicatedOnly, autoCloseServerCursor,
        tcpNoDelay, lazy, socketSendBuffer, socketReceiveBuffer, skipReducerOnUpdate, nestedTxMode,
        sslMode, sslCipherSuites, sslProtocol, sslKeyAlgorithm,
        sslClientCertificateKeyStoreUrl, sslClientCertificateKeyStorePassword, sslClientCertificateKeyStoreType,
        sslTrustCertificateKeyStoreUrl, sslTrustCertificateKeyStorePassword, sslTrustCertificateKeyStoreType,
        sslTrustAll, sslFactory,
        userAttrsFactory,
        user, passwd,
        dataPageScanEnabled,
            partitionAwareness,
        updateBatchSize,
            partitionAwarenessSQLCacheSize,
            partitionAwarenessPartDistributionsCacheSize,
        qryTimeout,
        connTimeout,
        disabledFeatures,
        keepBinary
    };

    /** {@inheritDoc} */
    @Override public String getSchema() {
        return schema.value();
    }

    /** {@inheritDoc} */
    @Override public void setSchema(String schema) {
        this.schema.setValue(schema);
    }

    /** {@inheritDoc} */
    @Override public String getUrl() {
        if (url != null)
            return url;
        else {
            if (F.isEmpty(getAddresses()))
                return null;

            StringBuilder sbUrl = new StringBuilder(JdbcThinUtils.URL_PREFIX);

            HostAndPortRange[] addrs = getAddresses();

            for (int i = 0; i < addrs.length; i++) {
                if (i > 0)
                    sbUrl.append(',');

                sbUrl.append(addrs[i].toString());
            }

            if (!F.isEmpty(getSchema()))
                sbUrl.append('/').append(getSchema());

            return sbUrl.toString();
        }
    }

    /** {@inheritDoc} */
    @Override public void setUrl(String url) throws SQLException {
        this.url = url;

        init(url, new Properties());
    }

    /** {@inheritDoc} */
    @Override public HostAndPortRange[] getAddresses() {
        return addrs;
    }

    /** {@inheritDoc} */
    @Override public void setAddresses(HostAndPortRange[] addrs) {
        this.addrs = addrs;
    }

    /** {@inheritDoc} */
    @Override public boolean isDistributedJoins() {
        return distributedJoins.value();
    }

    /** {@inheritDoc} */
    @Override public void setDistributedJoins(boolean val) {
        distributedJoins.setValue(val);
    }

    /** {@inheritDoc} */
    @Override public boolean isEnforceJoinOrder() {
        return enforceJoinOrder.value();
    }

    /** {@inheritDoc} */
    @Override public void setEnforceJoinOrder(boolean val) {
        enforceJoinOrder.setValue(val);
    }

    /** {@inheritDoc} */
    @Override public boolean isCollocated() {
        return collocated.value();
    }

    /** {@inheritDoc} */
    @Override public void setCollocated(boolean val) {
        collocated.setValue(val);
    }

    /** {@inheritDoc} */
    @Override public boolean isReplicatedOnly() {
        return replicatedOnly.value();
    }

    /** {@inheritDoc} */
    @Override public void setReplicatedOnly(boolean val) {
        replicatedOnly.setValue(val);
    }

    /** {@inheritDoc} */
    @Override public boolean isAutoCloseServerCursor() {
        return autoCloseServerCursor.value();
    }

    /** {@inheritDoc} */
    @Override public void setAutoCloseServerCursor(boolean val) {
        autoCloseServerCursor.setValue(val);
    }

    /** {@inheritDoc} */
    @Override public int getSocketSendBuffer() {
        return socketSendBuffer.value();
    }

    /** {@inheritDoc} */
    @Override public void setSocketSendBuffer(int size) throws SQLException {
        socketSendBuffer.setValue(size);
    }

    /** {@inheritDoc} */
    @Override public int getSocketReceiveBuffer() {
        return socketReceiveBuffer.value();
    }

    /** {@inheritDoc} */
    @Override public void setSocketReceiveBuffer(int size) throws SQLException {
        socketReceiveBuffer.setValue(size);
    }

    /** {@inheritDoc} */
    @Override public boolean isTcpNoDelay() {
        return tcpNoDelay.value();
    }

    /** {@inheritDoc} */
    @Override public void setTcpNoDelay(boolean val) {
        tcpNoDelay.setValue(val);
    }

    /** {@inheritDoc} */
    @Override public boolean isLazy() {
        return lazy.value();
    }

    /** {@inheritDoc} */
    @Override public void setLazy(boolean val) {
        lazy.setValue(val);
    }

    /** {@inheritDoc} */
    @Override public boolean isSkipReducerOnUpdate() {
        return skipReducerOnUpdate.value();
    }

    /** {@inheritDoc} */
    @Override public void setSkipReducerOnUpdate(boolean val) {
        skipReducerOnUpdate.setValue(val);
    }

    /** {@inheritDoc} */
    @Override public String getSslMode() {
        return sslMode.value();
    }

    /** {@inheritDoc} */
    @Override public void setSslMode(String mode) {
        sslMode.setValue(mode);
    }

    /** {@inheritDoc} */
    @Override public String getSslProtocol() {
        return sslProtocol.value();
    }

    /** {@inheritDoc} */
    @Override public void setSslProtocol(String sslProtocol) {
        this.sslProtocol.setValue(sslProtocol);
    }

    /** {@inheritDoc} */
    @Override public String getSslCipherSuites() {
        return sslCipherSuites.value();
    }

    /** {@inheritDoc} */
    @Override public void setSslCipherSuites(String sslCipherSuites) {
        this.sslCipherSuites.setValue(sslCipherSuites);
    }

    /** {@inheritDoc} */
    @Override public String getSslKeyAlgorithm() {
        return sslKeyAlgorithm.value();
    }

    /** {@inheritDoc} */
    @Override public void setSslKeyAlgorithm(String keyAlgorithm) {
        sslKeyAlgorithm.setValue(keyAlgorithm);
    }

    /** {@inheritDoc} */
    @Override public String getSslClientCertificateKeyStoreUrl() {
        return sslClientCertificateKeyStoreUrl.value();
    }

    /** {@inheritDoc} */
    @Override public void setSslClientCertificateKeyStoreUrl(String url) {
        sslClientCertificateKeyStoreUrl.setValue(url);
    }

    /** {@inheritDoc} */
    @Override public String getSslClientCertificateKeyStorePassword() {
        return sslClientCertificateKeyStorePassword.value();
    }

    /** {@inheritDoc} */
    @Override public void setSslClientCertificateKeyStorePassword(String passwd) {
        sslClientCertificateKeyStorePassword.setValue(passwd);
    }

    /** {@inheritDoc} */
    @Override public String getSslClientCertificateKeyStoreType() {
        return sslClientCertificateKeyStoreType.value();
    }

    /** {@inheritDoc} */
    @Override public void setSslClientCertificateKeyStoreType(String ksType) {
        sslClientCertificateKeyStoreType.setValue(ksType);
    }

    /** {@inheritDoc} */
    @Override public String getSslTrustCertificateKeyStoreUrl() {
        return sslTrustCertificateKeyStoreUrl.value();
    }

    /** {@inheritDoc} */
    @Override public void setSslTrustCertificateKeyStoreUrl(String url) {
        sslTrustCertificateKeyStoreUrl.setValue(url);
    }

    /** {@inheritDoc} */
    @Override public String getSslTrustCertificateKeyStorePassword() {
        return sslTrustCertificateKeyStorePassword.value();
    }

    /** {@inheritDoc} */
    @Override public void setSslTrustCertificateKeyStorePassword(String passwd) {
        sslTrustCertificateKeyStorePassword.setValue(passwd);
    }

    /** {@inheritDoc} */
    @Override public String getSslTrustCertificateKeyStoreType() {
        return sslTrustCertificateKeyStoreType.value();
    }

    /** {@inheritDoc} */
    @Override public void setSslTrustCertificateKeyStoreType(String ksType) {
        sslTrustCertificateKeyStoreType.setValue(ksType);
    }

    /** {@inheritDoc} */
    @Override public boolean isSslTrustAll() {
        return sslTrustAll.value();
    }

    /** {@inheritDoc} */
    @Override public void setSslTrustAll(boolean trustAll) {
        this.sslTrustAll.setValue(trustAll);
    }

    /** {@inheritDoc} */
    @Override public String getSslFactory() {
        return sslFactory.value();
    }

    /** {@inheritDoc} */
    @Override public void setSslFactory(String sslFactory) {
        this.sslFactory.setValue(sslFactory);
    }

    /** {@inheritDoc} */
    @Override public String nestedTxMode() {
        return nestedTxMode.value();
    }

    /** {@inheritDoc} */
    @Override public void nestedTxMode(String val) {
        nestedTxMode.setValue(val);
    }

    /** {@inheritDoc} */
    @Override public void setUsername(String name) {
        user.setValue(name);
    }

    /** {@inheritDoc} */
    @Override public String getUsername() {
        return user.value();
    }

    /** {@inheritDoc} */
    @Override public void setPassword(String passwd) {
        this.passwd.setValue(passwd);
    }

    /** {@inheritDoc} */
    @Override public String getPassword() {
        return passwd.value();
    }

    /** {@inheritDoc} */
    @Override public @Nullable Boolean isDataPageScanEnabled() {
        return dataPageScanEnabled.value();
    }

    /** {@inheritDoc} */
    @Override public void setDataPageScanEnabled(@Nullable Boolean dataPageScanEnabled) {
        this.dataPageScanEnabled.setValue(dataPageScanEnabled);
    }

    /** {@inheritDoc} */
    @Override public boolean isPartitionAwareness() {
        return partitionAwareness.value();
    }

    /** {@inheritDoc} */
    @Override public void setPartitionAwareness(boolean partitionAwareness) {
        this.partitionAwareness.setValue(partitionAwareness);
    }

    /** {@inheritDoc} */
    @Override public @Nullable Integer getUpdateBatchSize() {
        return updateBatchSize.value();
    }

    /** {@inheritDoc} */
    @Override public void setUpdateBatchSize(@Nullable Integer updateBatchSize) throws SQLException {
        this.updateBatchSize.setValue(updateBatchSize);
    }

    /** {@inheritDoc} */
    @Override public int getPartitionAwarenessSqlCacheSize() {
        return partitionAwarenessSQLCacheSize.value();
    }

    /** {@inheritDoc} */
    @Override public void setPartitionAwarenessSqlCacheSize(int partitionAwarenessSqlCacheSize)
        throws SQLException {
        this.partitionAwarenessSQLCacheSize.setValue(partitionAwarenessSqlCacheSize);
    }

    /** {@inheritDoc} */
    @Override public int getPartitionAwarenessPartitionDistributionsCacheSize() {
        return partitionAwarenessPartDistributionsCacheSize.value();
    }

    /** {@inheritDoc} */
    @Override public void setPartitionAwarenessPartitionDistributionsCacheSize(
        int partitionAwarenessPartDistributionsCacheSize) throws SQLException {
        this.partitionAwarenessPartDistributionsCacheSize.setValue(
                partitionAwarenessPartDistributionsCacheSize);
    }

    /** {@inheritDoc} */
    @Override public Integer getQueryTimeout() {
        return qryTimeout.value();
    }

    /** {@inheritDoc} */
    @Override public void setQueryTimeout(@Nullable Integer timeout) throws SQLException {
        qryTimeout.setValue(timeout);
    }

    /** {@inheritDoc} */
    @Override public int getConnectionTimeout() {
        return connTimeout.value();
    }

    /** {@inheritDoc} */
    @Override public void setConnectionTimeout(@Nullable Integer timeout) throws SQLException {
        connTimeout.setValue(timeout);
    }

    /** {@inheritDoc} */
    @Override public String getUserAttributesFactory() {
        return userAttrsFactory.value();
    }

    /** {@inheritDoc} */
    @Override public void setUserAttributesFactory(String cls) {
        userAttrsFactory.setValue(cls);
    }

    /** {@inheritDoc} */
    @Override public String disabledFeatures() {
        return disabledFeatures.value();
    }

    /** {@inheritDoc} */
    @Override public void disabledFeatures(String features) {
        disabledFeatures.setValue(features);
    }

    /** {@inheritDoc} */
    @Override public boolean isKeepBinary() {
        return keepBinary.value();
    }

    /** {@inheritDoc} */
    @Override public void setKeepBinary(boolean keepBinary) {
        this.keepBinary.setValue(keepBinary);
    }

    /**
     * @param url URL connection.
     * @param props Environment properties.
     * @throws SQLException On error.
     */
    public void init(String url, Properties props) throws SQLException {
        Properties props0 = (Properties)props.clone();

        if (!F.isEmpty(url))
            parseUrl(url, props0);

        for (ConnectionProperty aPropsArray : propsArray)
            aPropsArray.init(props0);

        if (!F.isEmpty(props.getProperty("user"))) {
            setUsername(props.getProperty("user"));
            setPassword(props.getProperty("password"));
        }
    }

    /**
     * Validates and parses connection URL.
     *
     * @param url URL.
     * @param props Properties.
     * @throws SQLException On error.
     */
    private void parseUrl(String url, Properties props) throws SQLException {
        if (F.isEmpty(url))
            throw new SQLException("URL cannot be null or empty.");

        if (!url.startsWith(JdbcThinUtils.URL_PREFIX))
            throw new SQLException("URL must start with \"" + JdbcThinUtils.URL_PREFIX + "\"");

        String nakedUrl = url.substring(JdbcThinUtils.URL_PREFIX.length()).trim();

        parseUrl0(nakedUrl, props);
    }

    /**
     * Parse naked URL (i.e. without {@link JdbcThinUtils#URL_PREFIX}).
     *
     * @param url Naked URL.
     * @param props Properties.
     * @throws SQLException If failed.
     */
    private void parseUrl0(String url, Properties props) throws SQLException {
        // Determine mode - semicolon or ampersand.
        int semicolonPos = url.indexOf(";");
        int slashPos = url.indexOf("/");
        int queryPos = url.indexOf("?");

        boolean semicolonMode;

        if (semicolonPos == -1 && slashPos == -1 && queryPos == -1)
            // No special char -> any mode could be used, choose semicolon for simplicity.
            semicolonMode = true;
        else {
            if (semicolonPos != -1) {
                // Use semicolon mode if it appears earlier than slash or query.
                semicolonMode =
                    (slashPos == -1 || semicolonPos < slashPos) && (queryPos == -1 || semicolonPos < queryPos);
            }
            else
                // Semicolon is not found.
                semicolonMode = false;
        }

        if (semicolonMode)
            parseUrlWithSemicolon(url, props);
        else
            parseUrlWithQuery(url, props);
    }

    /**
     * Parse URL in semicolon mode.
     *
     * @param url Naked URL
     * @param props Properties.
     * @throws SQLException If failed.
     */
    private void parseUrlWithSemicolon(String url, Properties props) throws SQLException {
        int pathPartEndPos = url.indexOf(';');

        if (pathPartEndPos == -1)
            pathPartEndPos = url.length();

        String pathPart = url.substring(0, pathPartEndPos);

        String paramPart = null;

        if (pathPartEndPos > 0 && pathPartEndPos < url.length())
            paramPart = url.substring(pathPartEndPos + 1, url.length());

        parseEndpoints(pathPart);

        if (!F.isEmpty(paramPart))
            parseParameters(paramPart, props, ";");
    }

    /**
     * Parse URL in query mode.
     *
     * @param url Naked URL
     * @param props Properties.
     * @throws SQLException If failed.
     */
    private void parseUrlWithQuery(String url, Properties props) throws SQLException {
        int pathPartEndPos = url.indexOf('?');

        if (pathPartEndPos == -1)
            pathPartEndPos = url.length();

        String pathPart = url.substring(0, pathPartEndPos);

        String paramPart = null;

        if (pathPartEndPos > 0 && pathPartEndPos < url.length())
            paramPart = url.substring(pathPartEndPos + 1, url.length());

        String[] pathParts = pathPart.split("/");

        parseEndpoints(pathParts[0]);

        if (pathParts.length > 2) {
            throw new SQLException("Invalid URL format (only schema name is allowed in URL path parameter " +
                "'host:port[/schemaName]'): " + this.url, SqlStateCode.CLIENT_CONNECTION_FAILED);
        }

        setSchema(pathParts.length == 2 ? pathParts[1] : null);

        if (!F.isEmpty(paramPart))
            parseParameters(paramPart, props, "&");
    }

    /**
     * Parse endpoints.
     *
     * @param endpointStr Endpoint string.
     * @throws SQLException If failed.
     */
    private void parseEndpoints(String endpointStr) throws SQLException {
        String[] endpoints = endpointStr.split(",");

        if (endpoints.length > 0)
            addrs = new HostAndPortRange[endpoints.length];

        for (int i = 0; i < endpoints.length; ++i ) {
            try {
                addrs[i] = HostAndPortRange.parse(endpoints[i],
                    ClientConnectorConfiguration.DFLT_PORT, ClientConnectorConfiguration.DFLT_PORT,
                    "Invalid endpoint format (should be \"host[:portRangeFrom[..portRangeTo]]\")");
            }
            catch (IgniteCheckedException e) {
                throw new SQLException(e.getMessage(), SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
        }

        if (F.isEmpty(addrs) || F.isEmpty(addrs[0].host()))
            throw new SQLException("Host name is empty", SqlStateCode.CLIENT_CONNECTION_FAILED);
    }

    /**
     * Validates and parses URL parameters.
     *
     * @param paramStr Parameters string.
     * @param props Properties.
     * @param delimChar Delimiter character.
     * @throws SQLException If failed.
     */
    private void parseParameters(String paramStr, Properties props, String delimChar) throws SQLException {
        StringTokenizer st = new StringTokenizer(paramStr, delimChar);

        boolean insideBrace = false;

        String key = null;
        String val = null;

        while (st.hasMoreTokens()) {
            String token = st.nextToken();

            if (!insideBrace) {
                int eqSymPos = token.indexOf('=');

                if (eqSymPos < 0) {
                    throw new SQLException("Invalid parameter format (should be \"key1=val1" + delimChar +
                        "key2=val2" + delimChar + "...\"): " + token);
                }

                if (eqSymPos == token.length())
                    throw new SQLException("Invalid parameter format (key and value cannot be empty): " + token);

                key = token.substring(0, eqSymPos);
                val = token.substring(eqSymPos + 1, token.length());

                if (val.startsWith("{")) {
                    val = val.substring(1);

                    insideBrace = true;
                }
            }
            else
                val += delimChar + token;

            if (val.endsWith("}")) {
                insideBrace = false;

                val = val.substring(0, val.length() - 1);
            }

            if (val.contains("{") || val.contains("}")) {
                throw new SQLException("Braces cannot be escaped in the value. " +
                    "Please use the connection Properties for such values. [property=" + key + ']');
            }

            if (!insideBrace) {
                if (key.isEmpty() || val.isEmpty())
                    throw new SQLException("Invalid parameter format (key and value cannot be empty): " + token);

                if (PROP_SCHEMA.equalsIgnoreCase(key))
                    setSchema(val);
                else
                    props.setProperty(PROP_PREFIX + key, val);
            }
        }
    }

    /**
     * @return Driver's properties info array.
     */
    public DriverPropertyInfo[] getDriverPropertyInfo() {
        DriverPropertyInfo[] infos = new DriverPropertyInfo[propsArray.length];

        for (int i = 0; i < propsArray.length; ++i)
            infos[i] = propsArray[i].getDriverPropertyInfo();

        return infos;
    }

    /**
     * @return Properties set contains connection parameters.
     */
    public Properties storeToProperties() {
        Properties props = new Properties();

        for (ConnectionProperty prop : propsArray) {
            if (prop.valueObject() != null)
                props.setProperty(PROP_PREFIX + prop.getName(), prop.valueObject());
        }

        return props;
    }

    /**
     *
     */
    private interface PropertyValidator extends Serializable {
        /**
         * @param val String representation of the property value to validate.
         * @throws SQLException On validation fails.
         */
        void validate(String val) throws SQLException;
    }

    /**
     *
     */
    private abstract static class ConnectionProperty implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Name. */
        protected String name;

        /** Property description. */
        protected String desc;

        /** Default value. */
        protected Object dfltVal;

        /**
         * An array of possible values if the value may be selected
         * from a particular set of values; otherwise null.
         */
        protected String[] choices;

        /** Required flag. */
        protected boolean required;

        /** Property validator. */
        protected PropertyValidator validator;

        /**
         * @param name Name.
         * @param desc Description.
         * @param dfltVal Default value.
         * @param choices Possible values.
         * @param required {@code true} if the property is required.
         */
        ConnectionProperty(String name, String desc, Object dfltVal, String[] choices, boolean required) {
            this.name = name;
            this.desc = desc;
            this.dfltVal = dfltVal;
            this.choices = choices;
            this.required = required;
        }

        /**
         * @param name Name.
         * @param desc Description.
         * @param dfltVal Default value.
         * @param choices Possible values.
         * @param required {@code true} if the property is required.
         * @param validator Property validator.
         */
        ConnectionProperty(String name, String desc, Object dfltVal, String[] choices, boolean required,
            PropertyValidator validator) {
            this.name = name;
            this.desc = desc;
            this.dfltVal = dfltVal;
            this.choices = choices;
            this.required = required;
            this.validator = validator;
        }

        /**
         * @return Default value.
         */
        Object getDfltVal() {
            return dfltVal;
        }

        /**
         * @return Property name.
         */
        String getName() {
            return name;
        }

        /**
         * @return Array of possible values if the value may be selected
         * from a particular set of values; otherwise null
         */
        String[] choices() {
            return choices;
        }

        /**
         * @param props Properties.
         * @throws SQLException On error.
         */
        void init(Properties props) throws SQLException {
            String strVal = props.getProperty(PROP_PREFIX + name);

            if (required && strVal == null) {
                throw new SQLException("Property '" + name + "' is required but not defined",
                    SqlStateCode.CLIENT_CONNECTION_FAILED);
            }

            if (validator != null)
                validator.validate(strVal);

            checkChoices(strVal);

            props.remove(name);

            init(strVal);
        }

        /**
         * @param strVal Checked value.
         * @throws SQLException On check error.
         */
        protected void checkChoices(String strVal) throws SQLException {
            if (strVal == null)
                return;

            if (choices != null) {
                for (String ch : choices) {
                    if (ch.equalsIgnoreCase(strVal))
                        return;
                }

                throw new SQLException("Invalid property value. [name=" + name + ", val=" + strVal
                    + ", choices=" + Arrays.toString(choices) + ']', SqlStateCode.CLIENT_CONNECTION_FAILED);
            }
        }

        /**
         * @param str String representation of the
         * @throws SQLException on error.
         */
        abstract void init(String str) throws SQLException;

        /**
         * @return String representation of the property value.
         */
        abstract String valueObject();

        /**
         * @return JDBC property info object.
         */
        DriverPropertyInfo getDriverPropertyInfo() {
            DriverPropertyInfo dpi = new DriverPropertyInfo(PROP_PREFIX + name, valueObject());

            dpi.choices = choices();
            dpi.required = required;
            dpi.description = desc;

            return dpi;
        }
    }

    /**
     *
     */
    private static class BooleanProperty extends ConnectionProperty {
        /** */
        private static final long serialVersionUID = 0L;

        /** Bool choices. */
        private static final String[] boolChoices = new String[] {Boolean.TRUE.toString(), Boolean.FALSE.toString()};

        /** Value. */
        private Boolean val;

        /**
         * @param name Name.
         * @param desc Description.
         * @param dfltVal Default value.
         * @param required {@code true} if the property is required.
         */
        BooleanProperty(String name, String desc, @Nullable Boolean dfltVal, boolean required) {
            super(name, desc, dfltVal, boolChoices, required);

            val = dfltVal;
        }

        /**
         * @return Property value.
         */
        @Nullable Boolean value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override void init(String str) throws SQLException {
            if (str == null)
                val = (Boolean)dfltVal;
            else {
                if (Boolean.TRUE.toString().equalsIgnoreCase(str))
                    val = true;
                else if (Boolean.FALSE.toString().equalsIgnoreCase(str))
                    val = false;
                else
                    throw new SQLException("Failed to parse boolean property [name=" + name +
                        ", value=" + str + ']', SqlStateCode.CLIENT_CONNECTION_FAILED);
            }
        }

        /** {@inheritDoc} */
        @Override String valueObject() {
            if (val == null)
                return null;

            return Boolean.toString(val);
        }

        /**
         * @param val Property value to set.
         */
        void setValue(Boolean val) {
            this.val = val;
        }
    }

    /**
     *
     */
    private abstract static class NumberProperty extends ConnectionProperty {
        /** */
        private static final long serialVersionUID = 0L;

        /** Value. */
        protected Number val;

        /** Allowed value range. */
        private Number[] range;

        /**
         * @param name Name.
         * @param desc Description.
         * @param dfltVal Default value.
         * @param required {@code true} if the property is required.
         * @param min Lower bound of allowed range.
         * @param max Upper bound of allowed range.
         */
        NumberProperty(String name, String desc, Number dfltVal, boolean required, Number min, Number max) {
            super(name, desc, dfltVal, null, required);

            val = dfltVal;

            range = new Number[] {min, max};
        }

        /** {@inheritDoc} */
        @Override void init(String str) throws SQLException {
            if (str == null)
                val = dfltVal != null ? (Number)dfltVal : null;
            else {
                try {
                    setValue(parse(str));
                }
                catch (NumberFormatException e) {
                    throw new SQLException("Failed to parse int property [name=" + name +
                        ", value=" + str + ']', SqlStateCode.CLIENT_CONNECTION_FAILED);
                }
            }
        }

        /**
         * @param str String value.
         * @return Number value.
         * @throws NumberFormatException On parse error.
         */
        protected abstract Number parse(String str) throws NumberFormatException;

        /** {@inheritDoc} */
        @Override String valueObject() {
            return val != null ? String.valueOf(val) : null;
        }

        /**
         * @param val Property value.
         * @throws SQLException On error.
         */
        void setValue(Number val) throws SQLException {
            if (range != null) {
                if (val.doubleValue() < range[0].doubleValue()) {
                    throw new SQLException("Property cannot be lower than " + range[0].toString() + " [name=" + name +
                        ", value=" + val.toString() + ']', SqlStateCode.CLIENT_CONNECTION_FAILED);
                }

                if (val.doubleValue() > range[1].doubleValue()) {
                    throw new SQLException("Property cannot be upper than " + range[1].toString() + " [name=" + name +
                        ", value=" + val.toString() + ']', SqlStateCode.CLIENT_CONNECTION_FAILED);
                }
            }

            this.val = val;
        }
    }

    /**
     *
     */
    private static class IntegerProperty extends NumberProperty {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param name Name.
         * @param desc Description.
         * @param dfltVal Default value.
         * @param required {@code true} if the property is required.
         * @param min Lower bound of allowed range.
         * @param max Upper bound of allowed range.
         */
        IntegerProperty(String name, String desc, Number dfltVal, boolean required, int min, int max) {
            super(name, desc, dfltVal, required, min, max);
        }

        /** {@inheritDoc} */
        @Override protected Number parse(String str) throws NumberFormatException {
            return Integer.parseInt(str);
        }

        /**
         * @return Property value.
         */
        Integer value() {
            return val != null ? val.intValue() : null;
        }
    }

    /**
     *
     */
    private static class StringProperty extends ConnectionProperty {
        /** */
        private static final long serialVersionUID = 0L;

        /** Value */
        private String val;

        /**
         * @param name Name.
         * @param desc Description.
         * @param dfltVal Default value.
         * @param choices Possible values.
         * @param required {@code true} if the property is required.
         * @param validator Property value validator.
         */
        StringProperty(String name, String desc, String dfltVal, String[] choices, boolean required,
            PropertyValidator validator) {
            super(name, desc, dfltVal, choices, required, validator);

            val = dfltVal;
        }

        /**
         * @param val Property value.
         */
        void setValue(String val) {
            this.val = val;
        }

        /**
         * @return Property value.
         */
        String value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override void init(String str) throws SQLException {
            if (validator != null)
                validator.validate(str);

            if (str == null)
                val = (String)dfltVal;
            else
                val = str;
        }

        /** {@inheritDoc} */
        @Override String valueObject() {
            return val;
        }
    }
}
