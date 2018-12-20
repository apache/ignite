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

package org.apache.ignite.internal.processors.metastorage.persistence;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/** */
class DistributedMetaStorageUpdateFuture extends GridFutureAdapter<Void> {
    /** */
    private final UUID id;

    /** */
    private final ConcurrentMap<UUID, GridFutureAdapter<?>> updateFuts;

    /** */
    public DistributedMetaStorageUpdateFuture(UUID id, ConcurrentMap<UUID, GridFutureAdapter<?>> updateFuts) {
        this.id = id;

        this.updateFuts = updateFuts;

        updateFuts.put(id, this);
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
        updateFuts.remove(id);

        return super.onDone(res, err);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DistributedMetaStorageUpdateFuture.class, this);
    }
}
