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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import org.apache.ignite.internal.IgniteInternalFuture;

/**
 * Keys to retry.
 */
public interface GridDhtFuture<T> extends IgniteInternalFuture<T> {
    /**
     * Node that future should be able to provide keys to retry before
     * it completes, so it's not necessary to wait till future is done
     * to get retry keys.
     *
     * @return Keys to retry because this node is no longer a primary or backup.
     */
    public Collection<Integer> invalidPartitions();
}