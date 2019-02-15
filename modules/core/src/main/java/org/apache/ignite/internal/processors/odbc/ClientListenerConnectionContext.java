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
    ClientListenerProtocolVersion currentVersion();

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
