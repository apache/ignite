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

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.Nullable;

/**
* Local fields query future.
*/
public class GridCacheLocalFieldsQueryFuture
    extends GridCacheLocalQueryFuture<Object, Object, List<Object>>
    implements GridCacheQueryMetadataAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** Meta data future. */
    private final GridFutureAdapter<List<GridQueryFieldMetadata>> metaFut;

    /**
     * @param ctx Cache context.
     * @param qry Query.
     */
    public GridCacheLocalFieldsQueryFuture(GridCacheContext<?, ?> ctx, GridCacheQueryBean qry) {
        super((GridCacheContext<Object, Object>)ctx, qry);

        metaFut = new GridFutureAdapter<>();

        if (!qry.query().includeMetadata())
            metaFut.onDone();
    }

    /**
     * @param nodeId Sender node ID.
     * @param metaData Meta data.
     * @param data Page data.
     * @param err Error.
     * @param finished Finished or not.
     */
    public void onFieldsPage(@Nullable UUID nodeId, @Nullable List<GridQueryFieldMetadata> metaData,
        @Nullable Collection<?> data, @Nullable Throwable err, boolean finished) {
        onPage(nodeId, null, data, err, finished);

        if (!metaFut.isDone())
            metaFut.onDone(metaData);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<List<GridQueryFieldMetadata>> metadata() {
        return metaFut;
    }

    /** {@inheritDoc} */
    @Override public boolean fields() {
        return true;
    }
}
