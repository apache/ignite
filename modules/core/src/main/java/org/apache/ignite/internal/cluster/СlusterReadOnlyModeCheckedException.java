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

package org.apache.ignite.internal.cluster;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCluster;
import org.jetbrains.annotations.Nullable;

/**
 * Exception defines modification data error in read-only cluster. See {@link IgniteCluster#readOnly()}
 */
public class СlusterReadOnlyModeCheckedException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Create empty exception.
     */
    public СlusterReadOnlyModeCheckedException() {
        // No-op.
    }

    /**
     * Creates new exception with given error message.
     *
     * @param msg Error message.
     */
    public СlusterReadOnlyModeCheckedException(String msg) {
        super(msg);
    }

    /**
     * Creates new grid exception with given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public СlusterReadOnlyModeCheckedException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates new exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     * @param writableStackTrace whether or not the stack trace should
     *                           be writable
     */
    public СlusterReadOnlyModeCheckedException(String msg, @Nullable Throwable cause, boolean writableStackTrace) {
        super(msg, cause, writableStackTrace);
    }

    /**
     * Creates new exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public СlusterReadOnlyModeCheckedException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }

    @Override public String toString() {
        return super.toString();
    }
}
