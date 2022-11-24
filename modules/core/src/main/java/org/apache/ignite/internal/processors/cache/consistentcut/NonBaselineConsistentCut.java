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

package org.apache.ignite.internal.processors.cache.consistentcut;

import java.util.UUID;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/** Describes Consistent Cut running on non-baseline nodes. */
class NonBaselineConsistentCut implements ConsistentCut {
    /** Consistent Cut ID. */
    @GridToStringInclude
    private final UUID id;

    /** Client nodes don't write to WAL, then complere Consistent Cut future at creation time. */
    private final IgniteInternalFuture<WALPointer> fut = new GridFinishedFuture<>();

    /** */
    NonBaselineConsistentCut(UUID id) {
        this.id = id;
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<WALPointer> consistentCutFuture() {
        return fut;
    }

    /** {@inheritDoc} */
    @Override public void cancel(Throwable err) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean baseline() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NonBaselineConsistentCut.class, this);
    }
}
