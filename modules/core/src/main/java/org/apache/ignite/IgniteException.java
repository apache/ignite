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

package org.apache.ignite;

import org.apache.ignite.internal.util.typedef.*;
import org.jetbrains.annotations.*;

import static org.apache.ignite.internal.util.IgniteUtils.*;

/**
 * General grid exception. This exception is used to indicate any error condition
 * within Grid.
 */
public class IgniteException extends RuntimeException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Create empty exception.
     */
    public IgniteException() {
        super();
    }

    /**
     * Creates new exception with given error message.
     *
     * @param msg Error message.
     */
    public IgniteException(String msg) {
        super(msg);
    }

    /**
     * Creates new grid exception with given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public IgniteException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Creates new exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IgniteException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }

    /**
     * Checks if this exception has given class in {@code 'cause'} hierarchy.
     *
     * @param cls Cause classes to check (if {@code null} or empty, {@code false} is returned).
     * @return {@code True} if one of the causing exception is an instance of passed in classes,
     *      {@code false} otherwise.
     */
    public boolean hasCause(@Nullable Class<? extends Throwable>... cls) {
        return X.hasCause(this, cls);
    }

    /**
     * Gets first exception of given class from {@code 'cause'} hierarchy if any.
     *
     * @param cls Cause class to get cause (if {@code null}, {@code null} is returned).
     * @return First causing exception of passed in class, {@code null} otherwise.
     */
    @Nullable public <T extends Throwable> T getCause(@Nullable Class<T> cls) {
        return X.cause(this, cls);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Adds troubleshooting links if they where not added by below in {@code cause} hierarchy.
     */
    @Override public String getMessage() {
        return X.hasCauseExcludeRoot(this, IgniteException.class, IgniteCheckedException.class) ?
            super.getMessage() : errorMessageWithHelpUrls(super.getMessage());
    }

    /**
     * Returns exception message.
     * <p>
     * Unlike {@link #getMessage()} this method never include troubleshooting links
     * to the result string.
     *
     * @return Original message.
     */
    public String getOriginalMessage() {
        return super.getMessage();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass() + ": " + getMessage();
    }
}
