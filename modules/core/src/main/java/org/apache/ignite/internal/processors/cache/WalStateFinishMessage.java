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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

/**
 * WAL state finish message.
 */
public class WalStateFinishMessage extends WalStateAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Whether WAL state was changed as a result of this call. */
    private final boolean changed;

    /** Error message. */
    private final String errMsg;

    /**
     * Constructor.
     *
     * @param opId Unique operation ID.
     * @param grpId Group ID.
     * @param grpDepId Group deployment ID.
     * @param changed Result.
     * @param errMsg Error message.
     */
    public WalStateFinishMessage(UUID opId, int grpId, IgniteUuid grpDepId, boolean changed, @Nullable String errMsg) {
        super(opId, grpId, grpDepId);

        this.changed = changed;
        this.errMsg = errMsg;
    }

    /**
     * @return Result.
     */
    public boolean changed() {
        return changed;
    }

    /**
     * @return Error message.
     */
    @Nullable public String errorMessage() {
        return errMsg;
    }

    /** {@inheritDoc} */
    @Override public boolean stopProcess() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WalStateFinishMessage.class, this, "super", super.toString());
    }
}
