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

package org.apache.ignite.internal.processors.query.schema;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Schema operation exception.
 */
public class SchemaOperationException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Code: generic error. */
    public static final int CODE_GENERIC = 0;

    /** Code: cache not found. */
    public static final int CODE_CACHE_NOT_FOUND = 1;

    /** Code: table not found. */
    public static final int CODE_TABLE_NOT_FOUND = 2;

    /** Code: table already exists. */
    public static final int CODE_TABLE_EXISTS = 3;

    /** Code: column not found. */
    public static final int CODE_COLUMN_NOT_FOUND = 4;

    /** Code: column already exists. */
    public static final int CODE_COLUMN_EXISTS = 5;

    /** Code: index not found. */
    public static final int CODE_INDEX_NOT_FOUND = 6;

    /** Code: index already exists. */
    public static final int CODE_INDEX_EXISTS = 7;

    /** Error code. */
    private final int code;

    /**
     * Constructor for specific error type.
     *
     * @param code Code.
     * @param objName Object name.
     */
    public SchemaOperationException(int code, String objName) {
        super(message(code, objName));

        this.code = code;
    }

    /**
     * Constructor for generic error.
     *
     * @param msg Message.
     */
    public SchemaOperationException(String msg) {
        this(msg, null);
    }

    /**
     * Constructor for generic error.
     *
     * @param msg Message.
     * @param cause Cause.
     */
    public SchemaOperationException(String msg, Throwable cause) {
        super(msg, cause);

        code = CODE_GENERIC;
    }

    /**
     * @return Code.
     */
    public int code() {
        return code;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaOperationException.class, this, "msg", getMessage());
    }

    /**
     * Create message for specific code and object name.
     *
     * @param code Code.
     * @param objName Object name.
     * @return Message.
     */
    private static String message(int code, String objName) {
        switch (code) {
            case CODE_CACHE_NOT_FOUND:
                return "Cache doesn't exist: " + objName;

            case CODE_TABLE_NOT_FOUND:
                return "Table doesn't exist: " + objName;

            case CODE_TABLE_EXISTS:
                return "Table already exists: " + objName;

            case CODE_COLUMN_NOT_FOUND:
                return "Column doesn't exist: " + objName;

            case CODE_COLUMN_EXISTS:
                return "Column already exists: " + objName;

            case CODE_INDEX_NOT_FOUND:
                return "Index doesn't exist: " + objName;

            case CODE_INDEX_EXISTS:
                return "Index already exists: " + objName;

            default:
                assert false;

                return null;
        }
    }
}
