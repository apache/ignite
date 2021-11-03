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

package org.apache.ignite.lang;

import org.jetbrains.annotations.Nullable;

/**
 * This exception is used in order to indicate that Ignite node is stopping (already stopped) for some reason.
 */
public class NodeStoppingException extends IgniteInternalCheckedException {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /**
     * Creates an empty node stopping exception.
     */
    public NodeStoppingException() {
        super("Operation has been cancelled (node is stopping).");
    }

    /**
     * Creates a new exception with the given error message.
     *
     * @param msg Error message.
     */
    public NodeStoppingException(String msg) {
        super(msg);
    }

    /**
     * Creates a new node stopping exception with the given throwable as a cause and source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public NodeStoppingException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Creates a new node stopping exception with the given error message and optional nested exception.
     *
     * @param msg                Error message.
     * @param cause              Optional nested exception (can be {@code null}).
     * @param writableStackTrace Whether or not the stack trace should be writable.
     */
    public NodeStoppingException(String msg, @Nullable Throwable cause, boolean writableStackTrace) {
        super(msg, cause, writableStackTrace);
    }

    /**
     * Creates a new node stopping exception with the given error message and optional nested exception.
     *
     * @param msg   Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public NodeStoppingException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
