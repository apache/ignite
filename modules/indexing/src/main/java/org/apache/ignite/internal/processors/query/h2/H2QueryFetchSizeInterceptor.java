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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.mxbean.SqlQueryMXBean;

/**
 * Print warning message to log when query result size fetch count is bigger then specified threshold.
 * Threshold may be recalculated with multiplier.
 *
 * @see SqlQueryMXBean
 */
public class H2QueryFetchSizeInterceptor {
    /** Query info. */
    private final IgniteLogger log;

    /** Query info. */
    private final H2QueryInfo qryInfo;

    /** Result set size threshold. */
    private long threshold;

    /** Result set size threshold multiplier. */
    private final int thresholdMult;

    /** Fetched count of rows. */
    private long fetchedSize;

    /** Big results flag. */
    private boolean bigResults;

    /**
     * @param h2 Indexing.
     * @param qryInfo Query will be print when fetch size will be greater than threshold.
     * @param log Logger to print warning.
     */
    public H2QueryFetchSizeInterceptor(IgniteH2Indexing h2, H2QueryInfo qryInfo, IgniteLogger log) {
        assert log != null;
        assert qryInfo != null;

        this.log = log;
        this.qryInfo = qryInfo;

        threshold = h2.longRunningQueries().getResultSetSizeThreshold();
        thresholdMult = h2.longRunningQueries().getResultSetSizeThresholdMultiplier();
    }

    /**
     *
     */
    public void checkOnFetchNext() {
        ++fetchedSize;

        if (threshold > 0 && fetchedSize >= threshold) {
            qryInfo.printLogMessage(log, "Query produced big result set. ",
                "fetched=" + fetchedSize);

            if (thresholdMult > 1)
                threshold *= thresholdMult;
            else
                threshold = 0;

            bigResults = true;
        }
    }

    /**
     *
     */
    public void checkOnClose() {
        if (bigResults) {
            qryInfo.printLogMessage(log, "Query produced big result set. ",
                "fetched=" + fetchedSize);
        }
    }
}
