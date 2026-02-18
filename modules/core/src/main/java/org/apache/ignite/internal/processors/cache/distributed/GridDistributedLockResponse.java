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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Lock response message.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class GridDistributedLockResponse extends GridDistributedBaseMessage {
    /** Future ID. */
    @Order(7)
    public IgniteUuid futId;

    /** Error. */
    @Order(8)
    public ErrorMessage errMsg;

    /** Values. */
    @GridToStringInclude
    @Order(9)
    public List<CacheObject> vals;

    /**
     * Empty constructor.
     */
    public GridDistributedLockResponse() {
        /* No-op. */
    }

    /**
     * @param cacheId Cache ID.
     * @param lockVer Lock version.
     * @param futId Future ID.
     * @param cnt Key count.
     * @param addDepInfo Deployment info.
     */
    public GridDistributedLockResponse(int cacheId,
        GridCacheVersion lockVer,
        IgniteUuid futId,
        int cnt,
        boolean addDepInfo) {
        super(lockVer, cnt, addDepInfo);

        assert futId != null;

        this.cacheId = cacheId;
        this.futId = futId;

        vals = new ArrayList<>(cnt);
    }

    /**
     * @param cacheId Cache ID.
     * @param lockVer Lock ID.
     * @param futId Future ID.
     * @param err Error.
     * @param addDepInfo Deployment info.
     */
    public GridDistributedLockResponse(int cacheId,
        GridCacheVersion lockVer,
        IgniteUuid futId,
        Throwable err,
        boolean addDepInfo) {
        super(lockVer, 0, addDepInfo);

        assert futId != null;

        this.cacheId = cacheId;
        this.futId = futId;
        errMsg = new ErrorMessage(err);
    }

    /**
     * @param cacheId Cache ID.
     * @param lockVer Lock ID.
     * @param futId Future ID.
     * @param cnt Count.
     * @param err Error.
     * @param addDepInfo Deployment info.
     */
    public GridDistributedLockResponse(int cacheId,
        GridCacheVersion lockVer,
        IgniteUuid futId,
        int cnt,
        Throwable err,
        boolean addDepInfo) {
        super(lockVer, cnt, addDepInfo);

        assert futId != null;

        this.cacheId = cacheId;
        this.futId = futId;
        errMsg = new ErrorMessage(err);

        vals = new ArrayList<>(cnt);
    }

    /**
     *
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @param futId New future ID.
     */
    public void futureId(IgniteUuid futId) {
        this.futId = futId;
    }

    /** {@inheritDoc} */
    @Override public Throwable error() {
        return ErrorMessage.error(errMsg);
    }

    /**
     * @return Error message.
     */
    public ErrorMessage errorMessage() {
        return errMsg;
    }

    /**
     * @param errMsg New error message.
     */
    public void errorMessage(ErrorMessage errMsg) {
        this.errMsg = errMsg;
    }

    /**
     * @return Values.
     */
    public List<CacheObject> values() {
        return vals;
    }

    /**
     * @param vals New values.
     */
    public void values(List<CacheObject> vals) {
        this.vals = vals;
    }

    /**
     * @param val Value.
     */
    public void addValue(CacheObject val) {
        vals.add(val);
    }

    /**
     * @return Values size.
     */
    protected int valuesSize() {
        return vals.size();
    }

    /**
     * @param idx Index.
     * @return Value for given index.
     */
    @Nullable public CacheObject value(int idx) {
        if (!F.isEmpty(vals))
            return vals.get(idx);

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext<?, ?> ctx) {
        return ctx.txLockMessageLogger();
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        prepareMarshalCacheObjects(vals, ctx.cacheContext(cacheId));
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        finishUnmarshalCacheObjects(vals, ctx.cacheContext(cacheId), ldr);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 22;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDistributedLockResponse.class, this,
            "super", super.toString());
    }
}
