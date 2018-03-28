/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * A response to {@link GridNearTxQueryResultsEnlistRequest}.
 */
public class GridNearTxQueryResultsEnlistResponse extends GridNearTxQueryEnlistResponse {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Default-constructor.
     */
    public GridNearTxQueryResultsEnlistResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache id.
     * @param futId Future id.
     * @param miniId Mini future id.
     * @param lockVer Lock version.
     * @param res Result.
     * @param err Error.
     */
    public GridNearTxQueryResultsEnlistResponse(int cacheId, IgniteUuid futId, int miniId, GridCacheVersion lockVer,
        long res,Throwable err) {
        super(cacheId, futId, miniId, lockVer, res, err);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 151;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxQueryResultsEnlistResponse.class, this);
    }
}
