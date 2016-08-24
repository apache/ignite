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

import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 * Provides initialized GridCacheReturn.
 */
public class GridCacheReturnCompletableWrapper {
    /** Initialized flag. */
    private volatile boolean init;

    /** Return value. */
    private final GridCacheReturn ret;

    /** Future. */
    private final AtomicReference<GridFutureAdapter<GridCacheReturn>> fut = new AtomicReference<>();

    /**
     * @param ret Return.
     */
    public GridCacheReturnCompletableWrapper(GridCacheReturn ret) {
        this.ret = ret;
    }

    /**
     * Marks as initialized.
     */
    public void markInitialized() {
        this.init = true;

        GridFutureAdapter<GridCacheReturn> fut = this.fut.get();

        if (fut != null)
            fut.onDone(ret);
    }

    /**
     * Allows wait for properly initialized value..
     */
    public IgniteInternalFuture<GridCacheReturn> fut() {
        GridFutureAdapter<GridCacheReturn> resFut;
        GridFutureAdapter<GridCacheReturn> oldFut = this.fut.get();

        if (oldFut != null)
            resFut = oldFut;
        else {
            GridFutureAdapter<GridCacheReturn> newFut = new GridFutureAdapter<>();

            fut.compareAndSet(null, newFut);

            resFut = fut.get();
        }

        if (this.init)
            resFut.onDone(ret);

        return resFut;
    }

    /**
     * Raw return value, possible not properly initialized yet.
     */
    public GridCacheReturn raw() {
        return ret;
    }
}
