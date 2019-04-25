/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.schema;

import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Index operation cancellation token.
 */
public class SchemaIndexOperationCancellationToken {
    /** Cancel flag. */
    private final AtomicBoolean flag = new AtomicBoolean();

    /**
     * Get cancel state.
     *
     * @return {@code True} if cancelled.
     */
    public boolean isCancelled() {
        return flag.get();
    }

    /**
     * Do cancel.
     *
     * @return {@code True} if cancel flag was set by this call.
     */
    public boolean cancel() {
        return flag.compareAndSet(false, true);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaIndexOperationCancellationToken.class, this);
    }
}
