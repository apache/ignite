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
    private Integer tcpPort;

    /** Context factory for SSL. */
    private String tcpSslCtxFactory;

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

        ConnectorConfiguration clnCfg = c.getConnectorConfiguration();

        restEnabled = clnCfg != null;

        if (restEnabled) {
            tcpSslEnabled = clnCfg.isSslEnabled();
            jettyPath = clnCfg.getJettyPath();
            jettyHost = getProperty(IGNITE_JETTY_HOST);
            jettyPort = intValue(IGNITE_JETTY_PORT, null);
            tcpHost = clnCfg.getHost();
            tcpPort = clnCfg.getPort();
            tcpSslCtxFactory = compactClass(clnCfg.getSslContextFactory());
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
    @Nullable public Integer getTcpPort() {
        return tcpPort;
    }

    /**
     * @return Context factory for SSL.
     */
    @Nullable public String getTcpSslContextFactory() {
        return tcpSslCtxFactory;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(restEnabled);
        out.writeBoolean(tcpSslEnabled);
        U.writeString(out, jettyPath);
        U.writeString(out, jettyHost);
        out.writeObject(jettyPort);
        U.writeString(out, tcpHost);
        out.writeObject(tcpPort);
        U.writeString(out, tcpSslCtxFactory);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        restEnabled = in.readBoolean();
        tcpSslEnabled = in.readBoolean();
        jettyPath = U.readString(in);
        jettyHost = U.readString(in);
        jettyPort = (Integer)in.readObject();
        tcpHost = U.readString(in);
        tcpPort = (Integer)in.readObject();
        tcpSslCtxFactory = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorRestConfiguration.class, this);
    }
}
