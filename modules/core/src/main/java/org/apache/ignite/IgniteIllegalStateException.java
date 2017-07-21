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

import org.apache.ignite.internal.util.typedef.X;
import org.jetbrains.annotations.Nullable;

/**
 * This exception indicates the ignite access in invalid state.
 */
public class IgniteIllegalStateException extends IllegalStateException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Constructs exception with given message and cause.
     *
     * @param msg Exception message.
     * @param cause Exception cause.
     */
    public IgniteIllegalStateException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }

    /**
     * Creates exception given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public IgniteIllegalStateException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Constructs exception with given message.
     *
     * @param msg Exception message.
     */
    public IgniteIllegalStateException(String msg) {
        super(msg);
    }

    /**
     * Checks if this exception has given class in {@code 'cause'} hierarchy.
     *
     * @param cls Cause class to check (if {@code null}, {@code false} is returned)..
     * @return {@code True} if one of the causing exception is an instance of passed in class,
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass() + ": " + getMessage();
    }
}