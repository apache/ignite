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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.ErrorMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/** Acknowledges removal of locks and transaction entries on an unlock recipient and its participants. */
public class GridCacheUnlockResponse extends GridCacheMessage {
    /** Request future ID. */
    @Order(0)
    public IgniteUuid futId;

    /** Cleanup error. */
    @Order(1)
    public ErrorMessage err;

    /** Required by the message factory. */
    public GridCacheUnlockResponse() {
        // No-op.
    }

    /**
     * @param futId Request future ID.
     * @param err Cleanup error.
     */
    public GridCacheUnlockResponse(IgniteUuid futId, @Nullable Throwable err) {
        this.futId = futId;
        this.err = err == null ? null : new ErrorMessage(err);
    }

    /** {@inheritDoc} */
    @Override public @Nullable Throwable error() {
        return ErrorMessage.error(err);
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheUnlockResponse.class, this, "super", super.toString());
    }
}
