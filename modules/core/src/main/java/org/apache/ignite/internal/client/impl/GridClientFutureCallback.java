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

package org.apache.ignite.internal.client.impl;

import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFuture;

/**
 * Future callback will be notified, when listened future finishes (both succeed or failed).
 * @param <R> Input parameter type.
 * @param <S> Result type.
 */
public interface GridClientFutureCallback<R, S> {
    /**
     * Future callback to executed when listened future finishes.
     *
     * @param fut Finished future to listen for.
     * @return Chained future result, if applicable, otherwise - {@code null}.
     */
    public S onComplete(GridClientFuture<R> fut) throws GridClientException;
}