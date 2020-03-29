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

package org.apache.ignite.internal.util.future;

import java.util.concurrent.Executor;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Wraps listener and executes it in specified executor.
 */
public class AsyncFutureListener<V> implements IgniteInClosure<IgniteFuture<V>> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgniteInClosure<? super IgniteFuture<V>> lsnr;

    /** */
    private final Executor exec;

    /**
     * @param lsnr Listener to be called asynchronously.
     * @param exec Executor to process listener.
     */
    public AsyncFutureListener(IgniteInClosure<? super IgniteFuture<V>> lsnr, Executor exec) {
        assert lsnr != null;
        assert exec != null;

        this.lsnr = lsnr;
        this.exec = exec;
    }

    /** {@inheritDoc} */
    @Override public void apply(final IgniteFuture<V> fut) {
        exec.execute(new Runnable() {
            @Override public void run() {
                lsnr.apply(fut);
            }
        });
    }
}
