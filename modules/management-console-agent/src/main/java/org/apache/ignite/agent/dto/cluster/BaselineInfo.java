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

package org.apache.ignite.agent.dto.cluster;

import java.util.Objects;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * DTO for baseline parameters.
 */
public class BaselineInfo {
    /** Is auto adjust enabled. */
    private boolean isAutoAdjustEnabled;

    /** Auto adjust awaiting time. */
    private long autoAdjustAwaitingTime;

    /**
     * Default constructor.
     */
    public BaselineInfo() {
        // No-op.
    }

    /**
     * @param isAutoAdjustEnabled Is auto adjust enabled.
     * @param autoAdjustAwaitingTime Auto adjust awaiting time.
     */
    public BaselineInfo(boolean isAutoAdjustEnabled, long autoAdjustAwaitingTime) {
        this.isAutoAdjustEnabled = isAutoAdjustEnabled;
        this.autoAdjustAwaitingTime = autoAdjustAwaitingTime;
    }

    /**
     * @return @{code True} if auto adjust is enabled.
     */
    public boolean isAutoAdjustEnabled() {
        return isAutoAdjustEnabled;
    }

    /**
     * @param autoAdjustEnabled Auto adjust enabled.
     * @return {@code This} for chaining method calls.
     */
    public BaselineInfo setAutoAdjustEnabled(boolean autoAdjustEnabled) {
        isAutoAdjustEnabled = autoAdjustEnabled;

        return this;
    }

    /**
     * @return Auto adjust awaiting time in ms.
     */
    public long getAutoAdjustAwaitingTime() {
        return autoAdjustAwaitingTime;
    }

    /**
     * @param autoAdjustAwaitingTime Auto adjust awaiting time.
     * @return {@code This} for chaining method calls.
     */
    public BaselineInfo setAutoAdjustAwaitingTime(long autoAdjustAwaitingTime) {
        this.autoAdjustAwaitingTime = autoAdjustAwaitingTime;

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        BaselineInfo that = (BaselineInfo) o;

        return isAutoAdjustEnabled == that.isAutoAdjustEnabled && autoAdjustAwaitingTime == that.autoAdjustAwaitingTime;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(isAutoAdjustEnabled, autoAdjustAwaitingTime);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BaselineInfo.class, this);
    }
}
