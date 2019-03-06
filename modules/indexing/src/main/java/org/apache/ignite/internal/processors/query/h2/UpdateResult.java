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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.sql.SQLException;
import java.util.Arrays;

import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.createJdbcSqlException;

/**
 * Update result - modifications count and keys to re-run query with, if needed.
 */
public final class UpdateResult {
    /** Result to return for operations that affected 1 item - mostly to be used for fast updates and deletes. */
    public static final UpdateResult ONE = new UpdateResult(1, X.EMPTY_OBJECT_ARRAY);

    /** Result to return for operations that affected 0 items - mostly to be used for fast updates and deletes. */
    public static final UpdateResult ZERO = new UpdateResult(0, X.EMPTY_OBJECT_ARRAY);

    /** Number of processed items. */
    private final long cnt;

    /** Keys that failed to be updated or deleted due to concurrent modification of values. */
    private final Object[] errKeys;

    /**
     * Constructor.
     *
     * @param cnt Updated rows count.
     * @param errKeys Array of erroneous keys.
     */
    public UpdateResult(long cnt, Object[] errKeys) {
        this.cnt = cnt;
        this.errKeys = U.firstNotNull(errKeys, X.EMPTY_OBJECT_ARRAY);
    }

    /**
     * @return Update counter.
     */
    public long counter() {
       return cnt;
    }

    /**
     * @return Error keys.
     */
    public Object[] errorKeys() {
        return errKeys;
    }

    /**
     * Check update result for erroneous keys and throws concurrent update exception if necessary.
     */
    public void throwIfError() {
        if (!F.isEmpty(errKeys)) {
            String msg = "Failed to update some keys because they had been modified concurrently " +
                "[keys=" + Arrays.toString(errKeys) + ']';

            SQLException conEx = createJdbcSqlException(msg, IgniteQueryErrorCode.CONCURRENT_UPDATE);

            throw new IgniteSQLException(conEx);
        }
    }
}
