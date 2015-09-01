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

package org.apache.ignite.spi.swapspace;

import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Utility wrapper class that represents swap key.
 * <p>
 * This class also holds information about partition this key belongs to
 * (if needed for caches).
 */
public class SwapKey {
    /** */
    @GridToStringInclude
    private final Object key;

    /** */
    private final int part;

    /** Serialized key. */
    @GridToStringExclude
    private byte[] keyBytes;

    /**
     * @param key Key.
     */
    public SwapKey(Object key) {
        this(key, Integer.MAX_VALUE, null);
    }

    /**
     * @param key Key.
     * @param part Partition.
     */
    public SwapKey(Object key, int part) {
        this(key, part, null);
    }

    /**
     * @param key Key.
     * @param part Part.
     * @param keyBytes Key bytes.
     */
    public SwapKey(Object key, int part, @Nullable byte[] keyBytes) {
        assert key != null;
        assert part >= 0;

        this.key = key;
        this.part = part;
        this.keyBytes = keyBytes;
    }

    /**
     * @return Key.
     */
    public Object key() {
        return key;
    }

    /**
     * @return Partition this key belongs to.
     */
    public int partition() {
        return part;
    }

    /**
     * @return Serialized key.
     */
    @Nullable public byte[] keyBytes() {
        return keyBytes;
    }

    /**
     * @param keyBytes Serialized key.
     */
    public void keyBytes(byte[] keyBytes) {
        assert keyBytes != null;

        this.keyBytes = keyBytes;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (obj instanceof SwapKey) {
            SwapKey other = (SwapKey)obj;

            return part == other.part && key.equals(other.key);
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return key.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SwapKey.class, this);
    }
}