/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.lang.IgniteAsyncSupport;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Adapter for {@link org.apache.ignite.lang.IgniteAsyncSupport}.
 */
public class AsyncSupportAdapter<T extends IgniteAsyncSupport> implements IgniteAsyncSupport {
    /** Future for previous asynchronous operation. */
    protected ThreadLocal<IgniteFuture<?>> curFut;

    /**
     * Default constructor.
     */
    public AsyncSupportAdapter() {
        // No-op.
    }

    /**
     * @param async Async enabled flag.
     */
    public AsyncSupportAdapter(boolean async) {
        if (async)
            curFut = new ThreadLocal<>();
    }

    /** {@inheritDoc} */
    @Override public T withAsync() {
        if (isAsync())
            return (T)this;

        return createAsyncInstance();
    }

    /**
     * Creates component with asynchronous mode enabled.
     *
     * @return Component with asynchronous mode enabled.
     */
    protected T createAsyncInstance() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        return curFut != null;
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> future() {
        return future(true);
    }

    /**
     * Gets and optionally resets future for previous asynchronous operation.
     *
     * @param reset Specifies whether to reset future.
     *
     * @return Future for previous asynchronous operation.
     */
    public <R> IgniteFuture<R> future(boolean reset) {
        if (curFut == null)
            throw new IllegalStateException("Asynchronous mode is disabled.");

        IgniteFuture<?> fut = curFut.get();

        if (fut == null)
            throw new IllegalStateException("Asynchronous operation not started.");

        if (reset)
            curFut.set(null);

        return (IgniteFuture<R>)fut;
    }

    /**
     * @param fut Future.
     * @return If async mode is enabled saves future and returns {@code null},
     *         otherwise waits for future and returns result.
     * @throws IgniteCheckedException If asynchronous mode is disabled and future failed.
     */
    public <R> R saveOrGet(IgniteInternalFuture<R> fut) throws IgniteCheckedException {
        if (curFut != null) {
            curFut.set(createFuture(fut));

            return null;
        }
        else
            return fut.get();
    }

    /**
     * @param fut Internal future.
     * @return Public API future.
     */
    protected <R> IgniteFuture<R> createFuture(IgniteInternalFuture<R> fut) {
        return new IgniteFutureImpl<>(fut);
    }
}