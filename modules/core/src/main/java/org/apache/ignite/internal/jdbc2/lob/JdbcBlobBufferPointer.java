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
    private long pos;

    /** Optional storage-specific context for effective access to data in the current position. */
    private JdbcBlobStorageContext context;

    /**
     * Create new pointer to zero position.
     */
    JdbcBlobBufferPointer() {
        pos = 0;
    }

    /**
     * Initialize pointer from the another one.
     *
     * @param pointer Another pointer.
     */
    JdbcBlobBufferPointer set(JdbcBlobBufferPointer pointer) {
        pos = pointer.pos;

        if (pointer.context != null)
            context = pointer.context.deepCopy();

        return this;
    }

    /**
     * Set current buffer position.
     *
     * @param pos New position.
     */
    JdbcBlobBufferPointer setPos(long pos) {
        this.pos = pos;

        return this;
    }

    /**
     * Set context.
     *
     * @param context New context.
     */
    JdbcBlobBufferPointer setContext(JdbcBlobStorageContext context) {
        this.context = context;

        return this;
    }

    /** @return Current buffer position. */
    long getPos() {
        return pos;
    }

    /** @return Context. */
    JdbcBlobStorageContext getContext() {
        return context;
    }
}
