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

import org.apache.ignite.internal.util.typedef.F;

/**
 * Key into cached H2 connections and statements.
 */
public class H2ConnectionCacheKey {
    /** Thread. */
    private final Thread thread;

    /** Schema name. */
    private final String schema;

    /**
     * Constructor.
     *
     * @param thread Thread.
     * @param schema Schema name.
     */
    public H2ConnectionCacheKey(Thread thread, String schema) {
        assert thread != null;

        this.thread = thread;
        this.schema = schema;
    }

    /**
     * @return Thread.
     */
    public Thread thread() {
        return thread;
    }

    /**
     * @return Schema name.
     */
    public String schema() {
        return schema;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof H2ConnectionCacheKey))
            return false;

        H2ConnectionCacheKey that = (H2ConnectionCacheKey)o;

        return thread == that.thread() && F.eq(schema, that.schema());
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * thread.hashCode() + (schema != null ? schema.hashCode() : 0);
    }
}
