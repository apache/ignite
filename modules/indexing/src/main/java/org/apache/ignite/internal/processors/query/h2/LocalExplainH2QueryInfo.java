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

import java.util.Collection;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Query info for special queries: EXPLAIN / SELECT PLAN.
 */
public class LocalExplainH2QueryInfo implements H2QueryInfo {
    /** Begin timestamp. */
    private final long beginTs;

    /** Sql. */
    private final String sql;

    /** Params. */
    private final Collection<Object> params;

    /**
     * @param sql Query statement.
     * @param params Query parameters.
     */
    public LocalExplainH2QueryInfo(String sql, Collection<Object> params) {
        this.sql = sql;
        this.params = params;
        beginTs = U.currentTimeMillis();
    }

    /** {@inheritDoc} */
    @Override public long time() {
        return U.currentTimeMillis() - beginTs;
    }

    /** {@inheritDoc} */
    @Override public void printLogMessage(IgniteLogger log, ConnectionManager connMgr, String msg) {
        LT.warn(log, msg + "[type=EXPLAIN, time=" + time() + ", sql=" + sql + ", params=" + params);
    }
}
