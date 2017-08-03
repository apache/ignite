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

package org.apache.ignite.internal.processors.query;

import java.sql.BatchUpdateException;
import java.sql.SQLException;

/**
 * Specific exception bearing information about update counts
 * for statements preceding the erroneous statement in a batch.
 */
public class IgniteSQLBatchUpdateException extends IgniteSQLException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Update counts. */
    private long[] updateCounts;

    /**
     * Constructor
     *
     * @param cause Cause of failure.
     * @param updateCounts update counts for statements preceding erroneous statement.
     */
    public IgniteSQLBatchUpdateException(SQLException cause, long[] updateCounts) {
        super(cause);

        this.updateCounts = updateCounts;
    }

    /**
     * @return Update counts.
     */
    public long[] updateCounts() {
        return updateCounts;
    }

    /**
     * @return JDBC exception containing details from this instance.
     */
    @Override public SQLException toJdbcException() {
        // Convert update counts to int
        int[] counts = new int[updateCounts.length];

        for (int idx = 0; idx < updateCounts.length; idx++)
            counts[idx] = (int)updateCounts[idx];

        return new BatchUpdateException(counts, getCause());
    }
}
