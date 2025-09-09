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

package org.apache.ignite.calcite;

import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.QueryEngineConfigurationEx;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Query engine configuration for Calcite-based query engine.
 */
@IgniteExperimental
public class CalciteQueryEngineConfiguration implements QueryEngineConfigurationEx {
    /** Query engine name. */
    public static final String ENGINE_NAME = "calcite";

    /** */
    private static final long DFLT_GLOBAL_MEMORY_QUOTA = 0L;

    /** */
    private static final long DFLT_QUERY_MEMORY_QUOTA = 0L;

    /** */
    private boolean isDflt;

    /** */
    private long globalMemoryQuota = DFLT_GLOBAL_MEMORY_QUOTA;

    /** */
    private long qryMemoryQuota = DFLT_QUERY_MEMORY_QUOTA;

    /** {@inheritDoc} */
    @Override public String engineName() {
        return ENGINE_NAME;
    }

    /** {@inheritDoc} */
    @Override public Class<? extends QueryEngine> engineClass() {
        return CalciteQueryProcessor.class;
    }

    /** {@inheritDoc} */
    @Override public boolean isDefault() {
        return isDflt;
    }

    /** {@inheritDoc} */
    @Override public CalciteQueryEngineConfiguration setDefault(boolean isDflt) {
        this.isDflt = isDflt;

        return this;
    }

    /**
     * Gets global heap memory quota for SQL engine.
     *
     * @return Global heap memory quota for SQL engine.
     */
    public long getGlobalMemoryQuota() {
        return globalMemoryQuota;
    }

    /**
     * Sets global heap memory quota for SQL engine.
     *
     * @param globalMemoryQuota Global heap memory quota for SQL engine.
     * @return {@code this} for chaining.
     */
    public CalciteQueryEngineConfiguration setGlobalMemoryQuota(long globalMemoryQuota) {
        this.globalMemoryQuota = globalMemoryQuota;

        return this;
    }

    /**
     * Gets per-query heap memory quota.
     *
     * @return Per-query heap memory quota.
     */
    public long getQueryMemoryQuota() {
        return qryMemoryQuota;
    }

    /**
     * Sets per-query heap memory quota.
     *
     * @param qryMemoryQuota Per-query heap memory quota.
     * @return {@code this} for chaining.
     */
    public CalciteQueryEngineConfiguration setQueryMemoryQuota(long qryMemoryQuota) {
        this.qryMemoryQuota = qryMemoryQuota;

        return this;
    }
}
