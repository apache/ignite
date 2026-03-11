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

package org.apache.ignite.internal.processors.query.calcite.message;

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CalciteErrorMessage implements CalciteMarshalableMessage {
    /** */
    @Order(0)
    UUID qryId;

    /** */
    @Order(1)
    long fragmentId;

    /** Error bytes. */
    @Order(2)
    @GridToStringExclude
    @Nullable public byte[] errBytes;

    /** Error. */
    private @Nullable Throwable err;

    /** */
    public CalciteErrorMessage() {
        // No-op.
    }

    /** */
    public CalciteErrorMessage(UUID qryId, long fragmentId, Throwable err) {
        assert err != null;

        this.qryId = qryId;
        this.fragmentId = fragmentId;
        this.err = err;
    }

    /**
     * @return Query ID.
     */
    public UUID queryId() {
        return qryId;
    }

    /**
     * @return Fragment ID.
     */
    public long fragmentId() {
        return fragmentId;
    }

    /** */
    public @Nullable Throwable error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.QUERY_ERROR_MESSAGE;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return MessageType.QUERY_ERROR_MESSAGE.directType();
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        errBytes = U.marshal(ctx.marshaller(), err);
    }

    /** {@inheritDoc} */
    @Override public void prepareUnmarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        err = U.unmarshal(ctx.marshaller(), errBytes, U.resolveClassLoader(ctx.cache().context().gridConfig()));
    }
}
