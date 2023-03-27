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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;

/**
 *
 */
public interface NearTxFinishFuture extends IgniteInternalFuture<IgniteInternalTx> {
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
