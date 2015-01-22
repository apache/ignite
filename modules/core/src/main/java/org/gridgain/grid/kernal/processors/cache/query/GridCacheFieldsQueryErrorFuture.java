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

package org.gridgain.grid.kernal.processors.cache.query;

import org.apache.ignite.internal.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.processors.query.*;
import org.apache.ignite.internal.util.future.*;

import java.util.*;

/**
* Error future for fields query.
*/
public class GridCacheFieldsQueryErrorFuture extends GridCacheQueryErrorFuture<List<?>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private boolean incMeta;

    /**
     * @param ctx Context.
     * @param th Error.
     * @param incMeta Include metadata flag.
     */
    public GridCacheFieldsQueryErrorFuture(GridKernalContext ctx, Throwable th, boolean incMeta) {
        super(ctx, th);

        this.incMeta = incMeta;
    }

    /**
     * @return Metadata.
     */
    public IgniteFuture<List<GridQueryFieldMetadata>> metadata() {
        return new GridFinishedFuture<>(ctx, incMeta ? Collections.<GridQueryFieldMetadata>emptyList() : null);
    }
}
