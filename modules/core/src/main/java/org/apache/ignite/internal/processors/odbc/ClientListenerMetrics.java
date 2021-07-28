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
package org.apache.ignite.internal.processors.odbc;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.IntMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;

import static org.apache.ignite.internal.processors.metric.GridMetricManager.CLIENT_METRICS;
import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.JDBC_CLIENT;
import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.ODBC_CLIENT;
import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.THIN_CLIENT;

/**
 * Client listener metrics.
 */
public class ClientListenerMetrics {
    /** Handshakes rejected by timeout metric label. */
    public static final String METRIC_REJECTED_TIMEOUT = "RejectedConnectionsByTimeout";

    /** Handshakes rejected by authentication metric label. */
    public static final String METRIC_REJECTED_AUTHENTICATION = "RejectedConnectionsDueAuthentication";

    /** Total number of rejected handshakes. */
    public static final String METRIC_REJECTED_TOTAL = "RejectedConnectionsTotal";

    /** Rejected by timeout. */
    private final IntMetricImpl rejectedTimeout;

    /** Rejected by authentication. */
    private final IntMetricImpl rejectedAuth;

    /** Total number of rejected connections. */
    private final IntMetricImpl rejectedTotal;

    /** Connections accepted. */
    private final Map<Byte, IntMetricImpl> accepted;

    /** Number of active connections. */
    private final Map<Byte, IntMetricImpl> active;

    /**
     * @param ctx Kernal context.
     */
    public ClientListenerMetrics(GridKernalContext ctx) {
        MetricRegistry mreg = ctx.metric().registry(CLIENT_METRICS);

        rejectedTimeout = mreg.intMetric(METRIC_REJECTED_TIMEOUT,
                "TCP sessions count that were rejected due to handshake timeout.");

        rejectedAuth = mreg.intMetric(METRIC_REJECTED_AUTHENTICATION,
                "TCP sessions count that were rejected due to failed authentication.");

        rejectedTotal = mreg.intMetric(METRIC_REJECTED_TOTAL, "Total number of rejected TCP connections.");

        final byte[] supportedClients = { ODBC_CLIENT, JDBC_CLIENT, THIN_CLIENT };
        accepted = new HashMap<>(supportedClients.length);
        active = new HashMap<>(supportedClients.length);

        for (byte clientType : supportedClients) {
            String clientLabel = clientTypeLabel(clientType);

            String labelAccepted = MetricUtils.metricName(clientLabel, "AcceptedConnections");
            accepted.put(clientType, mreg.intMetric(labelAccepted, "Number of successfully established sessions."));

            String labelActive = MetricUtils.metricName(clientLabel, "ActiveConnections");
            active.put(clientType, mreg.intMetric(labelActive, "Number of active sessions."));
        }
    }

    /**
     * Callback invoked when handshake is timed out.
     */
    public void onHandshakeTimeout() {
        rejectedTimeout.increment();
        rejectedTotal.increment();
    }

    /**
     * Callback invoked when authentication is failed.
     */
    public void onFailedAuth() {
        rejectedAuth.increment();
        rejectedTotal.increment();
    }

    /**
     * Callback invoked when handshake is rejected.
     */
    public void onGeneralReject() {
        rejectedTotal.increment();
    }

    /**
     * Callback invoked when handshake is accepted.
     *
     * @param clientType Client type.
     */
    public void onHandshakeAccept(byte clientType) {
        accepted.get(clientType).increment();
        active.get(clientType).increment();
    }

    /**
     * Callback invoked when client is disconnected.
     *
     * @param clientType Client type.
     */
    public void onDisconnect(byte clientType) {
        active.get(clientType).add(-1);
    }

    /**
     * Get label for a client.
     * @param clientType Client type.
     * @return Label for a client.
     */
    private String clientTypeLabel(byte clientType) {
        switch (clientType) {
            case ODBC_CLIENT:
                return "odbc";

            case JDBC_CLIENT:
                return "jdbc";

            case THIN_CLIENT:
                return "thin";

            default:
                return "unknown";
        }
    }
}
