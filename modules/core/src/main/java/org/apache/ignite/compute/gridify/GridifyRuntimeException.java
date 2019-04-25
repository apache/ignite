/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.compute.gridify;

import org.apache.ignite.IgniteException;

/**
 * This defines gridify exception. This runtime exception gets thrown out of gridified
 * methods in case if method execution resulted in undeclared exception.
 */
public class GridifyRuntimeException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates new gridify runtime exception with specified message.
     *
     * @param msg Exception message.
     */
    public GridifyRuntimeException(String msg) {
        super(msg);
    }

    /**
     * Creates new gridify runtime exception given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public GridifyRuntimeException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Creates new gridify runtime exception with specified message and cause.
     *
     * @param msg Exception message.
     * @param cause Exception cause.
     */
    public GridifyRuntimeException(String msg, Throwable cause) {
        super(msg, cause);
    }
}