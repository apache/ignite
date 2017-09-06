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

import org.apache.ignite.internal.binary.BinaryReaderExImpl;

/**
 * SQL listener connection context.
 */
public interface SqlListenerConnectionContext {
    /**
     * @param ver Version to check.
     * @return {@code true} if version is supported.
     */
    boolean isVersionSupported(SqlListenerProtocolVersion ver);

    /**
     * @return Current context version.
     */
    SqlListenerProtocolVersion currentVersion();

    /**
     * Initialize from handshake message.
     *
     * @param ver Protocol version.
     * @param reader Reader set to the configuration part of the handshake message.
     */
    void initializeFromHandshake(SqlListenerProtocolVersion ver, BinaryReaderExImpl reader);

    /**
     * Handler getter.
     * @return Request handler for the connection.
     */
    SqlListenerRequestHandler handler();

    /**
     * Parser getter
     * @return Message parser for the connection.
     */
    SqlListenerMessageParser parser();
}
