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

import java.util.Collection;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.jetbrains.annotations.Nullable;

/**
 * ODBC query execute with batch of parameters result.
 */
public class OdbcQueryExecuteBatchResult {
    /** Rows affected. */
    private final long[] affectedRows;

    /** Index of the set which caused an error. */
    private final long errorSetIdx;

    /** Error code. */
    private final int errorCode;

    /** Error message. */
    private final String errorMessage;

    /**
     * @param affectedRows Number of rows affected by the query.
     */
    public OdbcQueryExecuteBatchResult(long[] affectedRows) {
        this.affectedRows = affectedRows;
        this.errorSetIdx = -1;
        this.errorMessage = null;
        this.errorCode = ClientListenerResponse.STATUS_SUCCESS;
    }

    /**
     * @param affectedRows Number of rows affected by the query.
     * @param errorSetIdx Sets processed.
     * @param errorCode Error code.
     * @param errorMessage Error message.
     */
    public OdbcQueryExecuteBatchResult(long[] affectedRows, long errorSetIdx, int errorCode,
        String errorMessage) {
        this.affectedRows = affectedRows;
        this.errorSetIdx = errorSetIdx;
        this.errorMessage = errorMessage;
        this.errorCode = errorCode;
    }

    /**
     * @return Number of rows affected by the query.
     */
    public long[] affectedRows() {
        return affectedRows;
    }

    /**
     * @return Index of the set which caused an error or -1 if no error occurred.
     */
    public long errorSetIdx() {
        return errorSetIdx;
    }

    /**
     * @return Error message.
     */
    @Nullable public String errorMessage() {
        return errorMessage;
    }

    /**
     * @return Error code.
     */
    public int errorCode() {
        return errorCode;
    }
}
