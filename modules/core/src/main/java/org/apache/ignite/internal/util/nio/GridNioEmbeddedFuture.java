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

package org.apache.ignite.internal.util.nio;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Future that delegates to some other future.
 */
public class GridNioEmbeddedFuture<R> extends GridNioFutureImpl<R> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Callback to notify that future is finished.
     * This method must delegate to {@link #onDone(GridNioFuture, Throwable)} method.
     *
     * @param res Result.
     */
    public final void onDone(GridNioFuture<R> res) {
        onDone(res, null);
    }

    /**
     * Callback to notify that future is finished. Note that if non-{@code null} exception is passed in
     * the result value will be ignored.
     *
     * @param delegate Optional result.
     * @param err Optional error.
     */
    public void onDone(@Nullable GridNioFuture<R> delegate, @Nullable Throwable err) {
        assert delegate != null || err != null;

        if (err != null)
            onDone(err);
        else delegate.listen(new IgniteInClosure<IgniteInternalFuture<R>>() {
            @Override public void apply(IgniteInternalFuture<R> t) {
                try {
                    onDone(t.get());
                }
                catch (IgniteCheckedException e) {
                    onDone(e);
                }
            }
        });
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNioEmbeddedFuture.class, this, super.toString());
    }
}