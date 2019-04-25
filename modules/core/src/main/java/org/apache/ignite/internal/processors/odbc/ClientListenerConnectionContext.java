/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.jetbrains.annotations.Nullable;

/**
 * SQL listener connection context.
 */
public interface ClientListenerConnectionContext {
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
     * @param ver Protocol version.
     * @param reader Reader set to the configuration part of the handshake message.
     * @throws IgniteCheckedException On error.
     */
    void initializeFromHandshake(ClientListenerProtocolVersion ver, BinaryReaderExImpl reader)
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
     * Return connection authorization context.
     *
     * @return authorization context.
     */
    @Nullable AuthorizationContext authorizationContext();

    /**
     * @return Security context.
     */
    @Nullable SecurityContext securityContext();
}
