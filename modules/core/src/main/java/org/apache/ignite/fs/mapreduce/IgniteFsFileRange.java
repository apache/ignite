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

package org.apache.ignite.fs.mapreduce;

import org.apache.ignite.fs.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Entity representing part of GGFS file identified by file path, start position, and length.
 */
public class IgniteFsFileRange {
    /** File path. */
    private IgniteFsPath path;

    /** Start position. */
    private long start;

    /** Length. */
    private long len;

    /**
     * Creates file range.
     *
     * @param path File path.
     * @param start Start position.
     * @param len Length.
     */
    public IgniteFsFileRange(IgniteFsPath path, long start, long len) {
        this.path = path;
        this.start = start;
        this.len = len;
    }

    /**
     * Gets file path.
     *
     * @return File path.
     */
    public IgniteFsPath path() {
        return path;
    }

    /**
     * Gets range start position.
     *
     * @return Start position.
     */
    public long start() {
        return start;
    }

    /**
     * Gets range length.
     *
     * @return Length.
     */
    public long length() {
        return len;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteFsFileRange.class, this);
    }
}
