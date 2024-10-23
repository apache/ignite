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

package org.apache.ignite.internal.jdbc2.lob;

/**
 * Keeps a pointer to some position in a {@link JdbcBlobBuffer}.
 */
class JdbcBlobBufferPointer {
    /** Current buffer position. */
    private int pos;

    /** Index of the current buffer. */
    private Integer idx;

    /** Current position in the current buffer. */
    private Integer inBufPos;

    /**
     * Initialize pointer from the another one.
     *
     * @param pointer Another pointer.
     */
    JdbcBlobBufferPointer set(JdbcBlobBufferPointer pointer) {
        set(pointer.pos, pointer.idx, pointer.inBufPos);

        return this;
    }

    /**
     * Set current buffer position.
     *
     * @param pos New position.
     */
    JdbcBlobBufferPointer setPos(int pos) {
        this.pos = pos;

        return this;
    }

    /**
     * Set.
     *
     * @param pos New position.
     * @param idx Index of the current buffer.
     * @param inBufPos Current buffer position.
     */
    JdbcBlobBufferPointer set(int pos, Integer idx, Integer inBufPos) {
        this.pos = pos;
        this.idx = idx;
        this.inBufPos = inBufPos;

        return this;
    }

    /** @return Current buffer position. */
    int getPos() {
        return pos;
    }

    /** @return Index of the current buffer. */
    public Integer getIdx() {
        return idx;
    }

    /** @return Current buffer position. */
    public Integer getInBufPos() {
        return inBufPos;
    }
}
