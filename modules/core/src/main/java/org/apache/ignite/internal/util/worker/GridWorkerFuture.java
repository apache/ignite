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

package org.apache.ignite.internal.util.worker;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 * Future for locally executed closure that defines cancellation logic.
 */
public class GridWorkerFuture<T> extends GridFutureAdapter<T> {
    /** */
    private GridWorker w;

    /** {@inheritDoc} */
    @Override public boolean cancel() throws IgniteCheckedException {
        assert w != null;

        if (!onCancelled())
            return false;

        w.cancel();

        return true;
    }

    /**
     * @param w Worker.
     */
    public void setWorker(GridWorker w) {
        assert w != null;

        this.w = w;
    }
}