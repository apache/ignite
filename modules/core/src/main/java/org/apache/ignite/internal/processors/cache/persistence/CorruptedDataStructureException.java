/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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
package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract exception when {@link DataStructure} are corrupted.
 */
public abstract class CorruptedDataStructureException extends IgniteCheckedException {
    /** Cache group id. */
    protected final int grpId;

    /** PageId's that can be corrupted. */
    protected final long[] pageIds;

    /**
     * Constructor.
     *
     * @param msg Message.
     * @param cause Cause.
     * @param grpId Cache group id.
     * @param pageIds PageId's that can be corrupted.
     */
    protected CorruptedDataStructureException(String msg, @Nullable Throwable cause, int grpId, long[] pageIds) {
        super(msg, cause);

        this.grpId = grpId;
        this.pageIds = pageIds;
    }

    /**
     * @return Cache group id.
     */
    public long[] pageIds() {
        return pageIds;
    }

    /**
     * @return PageId's that can be corrupted.
     */
    public int groupId() {
        return grpId;
    }
}
