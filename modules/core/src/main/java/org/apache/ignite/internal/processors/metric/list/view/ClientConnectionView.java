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

package org.apache.ignite.internal.processors.metric.list.view;

import java.net.InetSocketAddress;
import org.apache.ignite.internal.processors.metric.list.MonitoringRow;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;

import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.JDBC_CLIENT;
import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.ODBC_CLIENT;
import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.THIN_CLIENT;

/** */
public class ClientConnectionView implements MonitoringRow<Long> {
    /** */
    private long id;

    /** */
    private byte type;

    /** */
    private InetSocketAddress rmtAddr;

    /** */
    private InetSocketAddress locAddr;

    /** */
    private String user;

    /** */
    private ClientListenerProtocolVersion ver;

    /**
     * @param id Id.
     * @param type Type.
     * @param rmtAddr Remote address.
     * @param locAddr Local address.
     * @param user User.
     * @param ver Version.
     */
    public ClientConnectionView(long id, byte type, InetSocketAddress rmtAddr, InetSocketAddress locAddr, String user,
        ClientListenerProtocolVersion ver) {
        this.id = id;
        this.type = type;
        this.rmtAddr = rmtAddr;
        this.locAddr = locAddr;
        this.user = user;
        this.ver = ver;
    }

    /** */
    public long id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public String sessionId() {
        return locAddr.toString();
    }

    /** */
    public String type() {
        switch (type) {
            case ODBC_CLIENT:
                return "ODBC";

            case JDBC_CLIENT:
                return "JDBC";

            case THIN_CLIENT:
                return "THIN";
        }

        return "Unknown client type: " + type;
    }

    /** */
    public InetSocketAddress localAddress() {
        return locAddr;
    }

    /** */
    public InetSocketAddress remoteAddress() {
        return rmtAddr;
    }

    /** */
    public String user() {
        return user == null ? user : "unknown";
    }

    /** */
    public ClientListenerProtocolVersion version() {
        return ver;
    }
}
