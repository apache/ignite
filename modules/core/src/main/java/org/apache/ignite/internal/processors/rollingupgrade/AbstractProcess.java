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

package org.apache.ignite.internal.processors.rollingupgrade;

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.Nullable;

/** */
abstract class AbstractProcess {
    /** */
    @Nullable private UUID activeOpId;

    /** */
    @Nullable private GridFutureAdapter<Void> activeOpFut;

    /** */
    protected abstract UUID startInternal() throws IgniteCheckedException;

    /** */
    public synchronized IgniteInternalFuture<Void> start() throws IgniteCheckedException {
        if (activeOpFut != null)
            return activeOpFut;

        activeOpFut = new GridFutureAdapter<>();
        activeOpId = startInternal();

        return activeOpFut;
    }

    /** */
    protected synchronized void finishProcess(UUID reqId, @Nullable Throwable err) {
        if (!isInitiator(reqId))
            return;

        activeOpFut.onDone(err);

        activeOpId = null;
        activeOpFut = null;
    }

    /** */
    private boolean isInitiator(UUID reqId) {
        return activeOpFut != null && reqId.equals(activeOpId);
    }
}
