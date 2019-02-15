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

/**
 * SQL state codes.
 */
public final class SqlStateCode {
    /**
     * No-op constructor to prevent instantiation.
     */
    private SqlStateCode() {
        // No-op.
    }

    /** Client has failed to open connection with specified server. */
    public final static String CLIENT_CONNECTION_FAILED = "08001";

    /** Connection unexpectedly turned out to be in closed state. */
    public final static String CONNECTION_CLOSED = "08003";

    /** Connection was rejected by server. */
    public final static String CONNECTION_REJECTED = "08004";

    /** IO error during communication. */
    public final static String CONNECTION_FAILURE = "08006";

    /** Null value occurred where it wasn't expected to. */
    public final static String NULL_VALUE = "22004";

    /** Parameter type is not supported. */
    public final static String INVALID_PARAMETER_VALUE = "22023";

    /** Data integrity constraint violation. */
    public final static String CONSTRAINT_VIOLATION = "23000";

    /** Invalid result set state. */
    public final static String INVALID_CURSOR_STATE = "24000";

    /** Conversion failure. */
    public final static String CONVERSION_FAILED = "0700B";

    /** Invalid transaction level. */
    public final static String INVALID_TRANSACTION_LEVEL = "0700E";

    /** Requested operation is not supported. */
    public final static String UNSUPPORTED_OPERATION = "0A000";

    /** Transaction state exception. */
    public final static String TRANSACTION_STATE_EXCEPTION = "25000";

    /** Transaction state exception. */
    public final static String SERIALIZATION_FAILURE = "40001";

    /** Parsing exception. */
    public final static String PARSING_EXCEPTION = "42000";

    /** Internal error. */
    public final static String INTERNAL_ERROR = "50000";  // Generic value for custom "50" class.
}
