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

import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.MANAGEMENT_CLIENT_ATTR;

/**
 * SQL listener connection context.
 */
public interface ClientListenerConnectionContext {
    /**
     * @return Client type.
     */
    byte clientType();

    /**
     * @return Current connection id.
     */
    long connectionId();

    /**
     * @param ver Version to check.
     * @return {@code true} if version is supported.
     */
    boolean isVersionSupported(ClientListenerProtocolVersion ver);

    /**
     * @return Current context version.
     */
    ClientListenerProtocolVersion defaultVersion();

    /**
     * Initialize from handshake message.
     *
     *
     * @param ses NIO session.
     * @param ver Protocol version.
     * @param reader Reader set to the configuration part of the handshake message.
     * @throws IgniteCheckedException On error.
     */
    void initializeFromHandshake(GridNioSession ses, ClientListenerProtocolVersion ver, BinaryReaderExImpl reader)
        throws IgniteCheckedException;

    /**
     * Handler getter.
     * @return Request handler for the connection.
     */
    ClientListenerRequestHandler handler();

    /**
     * Parser getter
     * @return Message parser for the connection.
     */
    ClientListenerMessageParser parser();

    /**
     * Called whenever client is disconnected due to correct connection close
     * or due to {@code IOException} during network operations.
     */
    void onDisconnected();

    /**
     * @return Security context.
     */
    @Nullable SecurityContext securityContext();

    /**
     * Connection attributes.
     */
    Map<String, String> attributes();

    /**
     * @return {@code True} if client is management.
     */
    default boolean isManagementClient() {
        return isManagementClient(attributes());
    }

    /**
     * @return {@code True} if client is management.
     */
    static boolean isManagementClient(Map<String, String> attributes) {
        return attributes != null && Boolean.parseBoolean(attributes.get(MANAGEMENT_CLIENT_ATTR));
    }
}
