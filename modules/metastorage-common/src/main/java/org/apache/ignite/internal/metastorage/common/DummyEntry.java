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

package org.apache.ignite.internal.metastorage.common;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.ignite.metastorage.common.Entry;
import org.apache.ignite.metastorage.common.Key;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

// TODO: IGNITE-14389 Tmp, used instead of real Entry implementation. Should be removed.
/**
 * Dummy entry implementation.
 */
public final class DummyEntry implements Entry, Serializable {
    /** Key. */
    @NotNull private Key key;

    /** Value. */
    @Nullable private byte[] val;

    /** Revision. */
    private long revision;

    /** Update counter. */
    private long updateCntr;

    /**
     *
     * @param key Key.
     * @param val Value.
     * @param revision Revision.
     * @param updateCntr Update counter.
     */
    public DummyEntry(@NotNull Key key, @Nullable byte[] val, long revision, long updateCntr) {
        this.key = key;
        this.val = val;
        this.revision = revision;
        this.updateCntr = updateCntr;
    }

    /** {@inheritDoc} */
    @Override public @NotNull Key key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public @Nullable byte[] value() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public long revision() {
        return revision;
    }

    /** {@inheritDoc} */
    @Override public long updateCounter() {
        return updateCntr;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DummyEntry entry = (DummyEntry)o;

        if (revision != entry.revision)
            return false;
        if (updateCntr != entry.updateCntr)
            return false;
        if (!key.equals(entry.key))
            return false;
        return Arrays.equals(val, entry.val);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = key.hashCode();
        res = 31 * res + Arrays.hashCode(val);
        res = 31 * res + (int)(revision ^ (revision >>> 32));
        res = 31 * res + (int)(updateCntr ^ (updateCntr >>> 32));
        return res;
    }
}
