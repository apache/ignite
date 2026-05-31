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

package org.apache.ignite.internal.processors.igfs.secondary.local;

import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.processors.igfs.IgfsBaseBlockKey;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;

/**
 * File's binary data block key.
 */
public final class LocalFileSystemBlockKey implements IgfsBaseBlockKey, Comparable<LocalFileSystemBlockKey> {
    /** IGFS path. */
    private IgfsPath path;

    /** Block ID. */
    private long blockId;

    /**
     * Constructs file's binary data block key.
     *
     * @param path IGFS path.
     * @param blockId Block ID.
     */
    public LocalFileSystemBlockKey(IgfsPath path, long blockId) {
        assert path != null;
        assert blockId >= 0;

        this.path = path;
        this.blockId = blockId;
    }

    /** {@inheritDoc} */
    @Override public long blockId() {
        return blockId;
    }

    /** {@inheritDoc} */
    @Override public int fileHash() {
        return path.hashCode();
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid affinityKey() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull LocalFileSystemBlockKey o) {
        int res = path.compareTo(o.path);

        if (res != 0)
            return res;

        long v1 = blockId;
        long v2 = o.blockId;

        if (v1 != v2)
            return v1 > v2 ? 1 : -1;

        return 0;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return path.hashCode() + (int)(blockId ^ (blockId >>> 32));
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o == this)
            return true;

        if (o == null || !(o instanceof LocalFileSystemBlockKey))
            return false;

        LocalFileSystemBlockKey that = (LocalFileSystemBlockKey)o;

        return blockId == that.blockId && path.equals(that.path);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(LocalFileSystemBlockKey.class, this);
    }
}
