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

package org.apache.ignite.internal.processors.odbc.odbc;

import org.apache.ignite.internal.processors.odbc.ClientListenerRequestNoId;

/**
 * SQL listener command request.
 */
public class OdbcRequest extends ClientListenerRequestNoId {
    /** Execute sql query. */
    public static final byte QRY_EXEC = 2;

    /** Fetch query results. */
    public static final byte QRY_FETCH = 3;

    /** Close query. */
    public static final byte QRY_CLOSE = 4;

    /** Get columns meta query. */
    public static final byte META_COLS = 5;

    /** Get columns meta query. */
    public static final byte META_TBLS = 6;

    /** Get parameters meta. */
    public static final byte META_PARAMS = 7;

    /** Execute sql query with the batch of parameters. */
    public static final byte QRY_EXEC_BATCH = 8;

    /** Get next result set. */
    public static final byte MORE_RESULTS = 9;

    /** Process ordered streaming batch. */
    public static final byte STREAMING_BATCH = 10;

    /** Command. */
    private final byte cmd;

    /**
     * @param cmd Command type.
     */
    public OdbcRequest(byte cmd) {
        this.cmd = cmd;
    }

    /**
     * @return Command.
     */
    public byte command() {
        return cmd;
    }
}
