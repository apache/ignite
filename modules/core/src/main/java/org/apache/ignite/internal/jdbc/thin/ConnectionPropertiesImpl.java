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
import javax.naming.RefAddr;
import javax.naming.Reference;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.util.typedef.F;

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

    /** Host name property. */
    private StringProperty host = new StringProperty(
        "host", "Ignite node IP to connect", null, null, true,
        new EmptyStringValidator("Host name is empty"));

    /** Connection port property. */
    private IntegerProperty port = new IntegerProperty(
        "port", "Ignite node IP to connect", ClientConnectorConfiguration.DFLT_PORT, false, 1, 0xFFFF);

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

    /** SSL: Use SSL connection to Ignite node. */
    private StringProperty sslMode = new StringProperty("sslMode",
        "The SSL mode of the connection", SSL_MODE_DISABLE,
        new String[] {SSL_MODE_DISABLE, SSL_MODE_REQUIRE}, false, null);

    /** SSL: Client certificate key store url. */
    private StringProperty sslProtocol = new StringProperty("sslProtocol",
        "SSL protocol name", null,  null, false, null);

    /** SSL: Key algorithm name. */
    private StringProperty sslKeyAlgorithm = new StringProperty("sslKeyAlgorithm",
        "SSL key algorithm name", "SunX509",  null, false, null);

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
        "Trust all certificates",false, false);

    /** SSL: Custom class name that implements Factory&lt;SSLSocketFactory&gt;. */
    private StringProperty sslFactory = new StringProperty("sslFactory",
        "Custom class name that implements Factory<SSLSocketFactory>", null, null, false, null);

    /** Properties array. */
    private final ConnectionProperty [] propsArray = {
        host, port,
        distributedJoins, enforceJoinOrder, collocated, replicatedOnly, autoCloseServerCursor,
        tcpNoDelay, lazy, socketSendBuffer, socketReceiveBuffer, skipReducerOnUpdate,
        sslMode, sslProtocol, sslKeyAlgorithm,
        sslClientCertificateKeyStoreUrl, sslClientCertificateKeyStorePassword, sslClientCertificateKeyStoreType,
        sslTrustCertificateKeyStoreUrl, sslTrustCertificateKeyStorePassword, sslTrustCertificateKeyStoreType,
        sslTrustAll, sslFactory
    };

    /** {@inheritDoc} */
    @Override public String getHost() {
        return host.value();
    }

    /** {@inheritDoc} */
    @Override public void setHost(String host) {
        this.host.setValue(host);
    }

    /** {@inheritDoc} */
    @Override public int getPort() {
        return port.value();
    }

    /** {@inheritDoc} */
    @Override public void setPort(int port) throws SQLException {
        this.port.setValue(port);
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

    /**
     * @param props Environment properties.
     * @throws SQLException On error.
     */
    void init(Properties props) throws SQLException {
        Properties props0 = (Properties)props.clone();

        for (ConnectionProperty aPropsArray : propsArray)
            aPropsArray.init(props0);
    }

    /**
     * @return Driver's properties info array.
     */
    private DriverPropertyInfo[] getDriverPropertyInfo() {
        DriverPropertyInfo[] dpis = new DriverPropertyInfo[propsArray.length];

        for (int i = 0; i < propsArray.length; ++i)
            dpis[i] = propsArray[i].getDriverPropertyInfo();

        return dpis;
    }

    /**
     * @param props Environment properties.
     * @return Driver's properties info array.
     * @throws SQLException On error.
     */
    public static DriverPropertyInfo[] getDriverPropertyInfo(Properties props) throws SQLException {
        ConnectionPropertiesImpl cpi = new ConnectionPropertiesImpl();

        cpi.init(props);

        return cpi.getDriverPropertyInfo();
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
    private static class EmptyStringValidator implements PropertyValidator {
        /** */
        private static final long serialVersionUID = 0L;

        /** Error message. */
        private final String errMsg;

        /**
         * @param msg Error message.
         */
        private EmptyStringValidator(String msg) {
            errMsg = msg;
        }

        /** {@inheritDoc} */
        @Override public void validate(String val) throws SQLException {
            if (F.isEmpty(val))
                throw new SQLException(errMsg, SqlStateCode.CLIENT_CONNECTION_FAILED);
        }
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
        protected String [] choices;

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
            this.desc= desc;
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
            this.desc= desc;
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

            checkChoices(strVal);

            if (validator != null)
                validator.validate(strVal);

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
         * @param ref Reference object.
         * @throws SQLException On error.
         */
        void init(Reference ref) throws SQLException {
            RefAddr refAddr = ref.get(name);

            if (refAddr != null) {
                String str = (String) refAddr.getContent();

                if (validator != null)
                    validator.validate(str);

                init(str);
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
            DriverPropertyInfo dpi = new DriverPropertyInfo(name, valueObject());

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
        private static final String [] boolChoices = new String[] {Boolean.TRUE.toString(), Boolean.FALSE.toString()};

        /** Value. */
        private boolean val;

        /**
         * @param name Name.
         * @param desc Description.
         * @param dfltVal Default value.
         * @param required {@code true} if the property is required.
         */
        BooleanProperty(String name, String desc, boolean dfltVal, boolean required) {
            super(name, desc, dfltVal, boolChoices, required);

            val = dfltVal;
        }

        /**
         * @return Property value.
         */
        boolean value() {
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
            return Boolean.toString(val);
        }

        /**
         * @param val Property value to set.
         */
        void setValue(boolean val) {
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
        private Number [] range;

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

            assert dfltVal != null;

            val = dfltVal;

            range = new Number[] {min, max};
        }

        /** {@inheritDoc} */
        @Override void init(String str) throws SQLException {
            if (str == null)
                val = (int)dfltVal;
            else {
                try {
                    setValue(parse(str));
                } catch (NumberFormatException e) {
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
            return String.valueOf(val);
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
        int value() {
            return val.intValue();
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
        StringProperty(String name, String desc, String dfltVal, String [] choices, boolean required,
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
