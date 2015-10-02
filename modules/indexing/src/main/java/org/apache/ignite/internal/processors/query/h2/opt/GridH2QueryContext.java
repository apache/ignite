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

package org.apache.ignite.internal.processors.query.h2.opt;

import org.apache.ignite.spi.indexing.IndexingQueryFilter;

/**
 * Thread local SQL query context which is intended to be accessible from everywhere.
 */
public class GridH2QueryContext {
    /** */
    private static final ThreadLocal<GridH2QueryContext> ctx = new ThreadLocal<>();

    /** */
    private IndexingQueryFilter filter;

    /**
     * Creates current thread local context.
     */
    public static GridH2QueryContext create() {
        assert ctx.get() == null;

        GridH2QueryContext res = new GridH2QueryContext();

        ctx.set(res);

        return res;
    }

    /**
     * Destroys current thread local context.
     */
    public static void destroy() {
        assert ctx.get() != null;

        ctx.remove();
    }

    /**
     * Must be called only after {@link #create()} and before {@link #destroy()}.
     *
     * @return Current thread local context.
     */
    public static GridH2QueryContext get() {
        GridH2QueryContext res = ctx.get();

        assert res != null;

        return res;
    }

    /**
     * @return Filter.
     */
    public IndexingQueryFilter filter() {
        return filter;
    }

    /**
     * @param filter Filter.
     */
    public void filter(IndexingQueryFilter filter) {
        this.filter = filter;
    }
}
