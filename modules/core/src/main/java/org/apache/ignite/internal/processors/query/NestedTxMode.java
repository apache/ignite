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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.IgniteException;

/**
 * Behavior options when an attempt to start a nested transaction is made.
 */
public enum NestedTxMode {
    /** Previously started transaction will be committed, new transaction will be started. */
    COMMIT,

    /** Warning will be printed to log, no new transaction will be started. */
    IGNORE,

    /** Exception will be thrown, previously started transaction will be rolled back. */
    ERROR;

    /** Default handling mode. */
    public final static NestedTxMode DEFAULT = ERROR;

    /**
     * Get enum value from int
     *
     * @param val Int value.
     * @return Enum value.
     * @throws IgniteException if the is no enum value associated with the int value.
     */
    public static NestedTxMode fromByte(byte val) {
        switch (val) {
            case 1:
                return COMMIT;

            case 2:
                return IGNORE;

            case 3:
                return ERROR;

            default:
                throw new IgniteException("Invalid nested transactions handling mode: " + val);
        }
    }
}
