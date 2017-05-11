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

/**
 * SQL listener connection context.
 */
public class SqlListenerConnectionContext {
    /** Request handler. */
    private final SqlListenerRequestHandler handler;

    /** Message parser. */
    private final SqlListenerMessageParser parser;

    /**
     * Constructor.
     *
     * @param handler Handler.
     * @param parser Parser.
     */
    public SqlListenerConnectionContext(SqlListenerRequestHandler handler, SqlListenerMessageParser parser) {
        this.handler = handler;
        this.parser = parser;
    }

    /**
     * Handler getter.
     * @return Request handler for the connection.
     */
    public SqlListenerRequestHandler handler() {
        return handler;
    }

    /**
     * Parser getter
     * @return Message parser for the connection.
     */
    public SqlListenerMessageParser parser() {
        return parser;
    }
}
