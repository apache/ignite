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

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.Executor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Implementation of public API future for cache.
 */
public class IgniteCacheFutureImpl<V> extends IgniteFutureImpl<V> {
    /**
     * Constructor.
     *
     * @param fut Internal future.
     */
    public IgniteCacheFutureImpl(IgniteInternalFuture<V> fut) {
        super(fut);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<T> chain(IgniteClosure<? super IgniteFuture<V>, T> doneCb) {
        return new IgniteCacheFutureImpl<>(chainInternal(doneCb, null));
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<T> chainAsync(IgniteClosure<? super IgniteFuture<V>, T> doneCb, Executor exec) {
        return new IgniteCacheFutureImpl<>(chainInternal(doneCb, exec));
    }

    /** {@inheritDoc} */
    @Override protected RuntimeException convertException(IgniteCheckedException e) {
        if (e instanceof IgniteFutureCancelledCheckedException ||
            e instanceof IgniteInterruptedCheckedException ||
            e instanceof IgniteFutureTimeoutCheckedException)
            return U.convertException(e);

        return CU.convertToCacheException(e);
    }
}