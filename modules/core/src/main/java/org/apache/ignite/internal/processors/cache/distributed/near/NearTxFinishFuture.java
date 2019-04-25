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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;

/**
 *
 */
public interface NearTxFinishFuture extends IgniteInternalFuture<IgniteInternalTx>  {
    /**
     * @return Commit flag.
     */
    boolean commit();

    /**
     * @return Transaction.
     */
    GridNearTxLocal tx();

    /**
     *
     * @param commit Commit flag.
     * @param clearThreadMap If {@code true} removes {@link GridNearTxLocal} from thread map.
     * @param onTimeout If {@code true} called from timeout handler.
     */
    public void finish(boolean commit, boolean clearThreadMap, boolean onTimeout);

    /**
     * @param e Error.
     */
    public void onNodeStop(IgniteCheckedException e);
}
