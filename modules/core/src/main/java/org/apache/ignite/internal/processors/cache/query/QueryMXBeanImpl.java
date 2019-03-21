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

package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.internal.GridKernalContextImpl;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.mxbean.QueryMXBean;

/**
 * QueryMXBean implementation.
 */
public class QueryMXBeanImpl implements QueryMXBean {
    /** */
    private final GridKernalContextImpl ctx;

    /**
     * @param ctx Context.
     */
    public QueryMXBeanImpl(GridKernalContextImpl ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public long getLongQueryWarningTimeout() {
        return ctx.query().getIndexing().getLongQueryWarningTimeout();
    }

    /** {@inheritDoc} */
    @Override public void setLongQueryWarningTimeout(long longQueryWarningTimeout) {
        ctx.query().getIndexing().setLongQueryWarningTimeout(longQueryWarningTimeout);
    }

    /** {@inheritDoc} */
    @Override public long getResultSetSizeThreshold() {
        return ctx.query().getIndexing().getResultSetSizeThreshold();
    }

    /** {@inheritDoc} */
    @Override public void setResultSetSizeThreshold(long resultSetSizeThreshold) {
        ctx.query().getIndexing().setResultSetSizeThreshold(resultSetSizeThreshold);
    }
}
