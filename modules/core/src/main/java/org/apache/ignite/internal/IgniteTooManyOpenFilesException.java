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

package org.apache.ignite.internal;

import java.net.SocketException;
import org.apache.ignite.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Custom exception for {@link SocketException} with Too many open files error message. It needs for failing node, if we
 * got that exception.
 */
public class IgniteTooManyOpenFilesException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Create empty exception.
     */
    public IgniteTooManyOpenFilesException() {
        super();
    }

    /**
     * Creates new exception with given error message.
     *
     * @param msg Error message.
     */
    public IgniteTooManyOpenFilesException(String msg) {
        super(msg);
    }

    /**
     * Creates new exception with given throwable as a cause and source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public IgniteTooManyOpenFilesException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates new exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IgniteTooManyOpenFilesException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
