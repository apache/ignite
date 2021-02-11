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

package org.apache.ignite.internal.processors.query.h2;

import java.sql.SQLException;
import java.util.Arrays;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionResult;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

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

    /** Partition result. */
    private final PartitionResult partRes;

    /**
     * Constructor.
     *
     * @param cnt Updated rows count.
     * @param errKeys Array of erroneous keys.
     */
    public UpdateResult(long cnt, Object[] errKeys) {
        this.cnt = cnt;
        this.errKeys = U.firstNotNull(errKeys, X.EMPTY_OBJECT_ARRAY);
        partRes = null;
    }

    /**
     * Constructor.
     *
     * @param cnt Updated rows count.
     * @param errKeys Array of erroneous keys.
     * @param partRes Partition result.
     */
    public UpdateResult(long cnt, Object[] errKeys, PartitionResult partRes) {
        this.cnt = cnt;
        this.errKeys = U.firstNotNull(errKeys, X.EMPTY_OBJECT_ARRAY);
        this.partRes = partRes;
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

    /**
     * @return Partition result.
     */
    public PartitionResult partitionResult() {
        return partRes;
    }
}
