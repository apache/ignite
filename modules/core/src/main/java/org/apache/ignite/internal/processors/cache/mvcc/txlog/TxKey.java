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

package org.apache.ignite.internal.processors.cache.mvcc.txlog;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class TxKey {
    /** */
    private final long major;

    /** */
    private final long minor;

    /**
     * @param major Major version.
     * @param minor Minor version
     */
    public TxKey(long major, long minor) {
        this.major = major;
        this.minor = minor;
    }

    /**
     * @return Major version.
     */
    public long major() {
        return major;
    }

    /**
     * @return Minor version.
     */
    public long minor() {
        return minor;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || o.getClass() != TxKey.class) return false;

        TxKey txKey = (TxKey) o;

        return major == txKey.major && minor == txKey.minor;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = (int) (major ^ (major >>> 32));
        result = 31 * result + (int) (minor ^ (minor >>> 32));
        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TxKey.class, this);
    }
}
