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
package org.apache.ignite.internal.processors.query.calcite.exec;

import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexQueryContext;
import org.apache.ignite.internal.util.lang.GridCursor;

/**
 * Tree index interface.
 *
 * @param <R> Indexing row type.
 */
public interface TreeIndex<R> {
    /**
     * Index lookup method.
     *
     * @param lower Lower bound.
     * @param upper Upper bound.
     * @param lowerInclude Inclusive lower bound.
     * @param upperInclude Inclusive upper bound.
     * @param qctx Index query context.
     * @return Cursor over the rows within bounds.
     */
    public GridCursor<R> find(R lower, R upper, boolean lowerInclude, boolean upperInclude, IndexQueryContext qctx);
}
