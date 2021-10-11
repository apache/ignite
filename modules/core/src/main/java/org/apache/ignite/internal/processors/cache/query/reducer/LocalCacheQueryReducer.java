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

package org.apache.ignite.internal.processors.cache.query.reducer;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.F;

/** Simple reducer for local queries. */
public class LocalCacheQueryReducer<R> extends CacheQueryReducer<R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Single page. */
    private NodePage<R> page;

    /** Local node stream with single page. */
    private final NodePageStream<R> stream;

    /** */
    public LocalCacheQueryReducer(NodePageStream<R> stream) {
        super(F.asMap(stream.nodeId(), stream));

        this.stream = stream;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNextX() throws IgniteCheckedException {
        if (page == null)
            page = page(stream.headPage());

        return page.hasNext();
    }

    /** {@inheritDoc} */
    @Override public R nextX() throws IgniteCheckedException {
        return page.next();
    }
}
