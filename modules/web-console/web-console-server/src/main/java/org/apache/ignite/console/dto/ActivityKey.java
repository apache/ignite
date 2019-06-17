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

package org.apache.ignite.console.dto;

import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Activity info.
 */
public class ActivityKey {
    /** */
    private UUID owner;

    /** */
    private long date;

    /**
     * Default constructor for serialization.
     */
    public ActivityKey() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param owner Owner ID.
     * @param date Activity period (year and month).
     */
    public ActivityKey(UUID owner, long date) {
        this.owner = owner;
        this.date = date;
    }

    /**
     * @return Owner.
     */
    public UUID getOwner() {
        return owner;
    }

    /**
     * @param owner Owner.
     */
    public void setOwner(UUID owner) {
        this.owner = owner;
    }

    /**
     * @return Activity period.
     */
    public long getDate() {
        return date;
    }

    /**
     * @param date Activity period.
     */
    public void setDate(long date) {
        this.date = date;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        ActivityKey key = (ActivityKey)obj;

        return date == key.date && Objects.equals(owner, key.owner);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(owner, date);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ActivityKey.class, this);
    }
}
