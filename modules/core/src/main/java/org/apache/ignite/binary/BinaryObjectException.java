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

package org.apache.ignite.binary;

import org.apache.ignite.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Exception indicating binary object serialization error.
 */
public class BinaryObjectException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates binary exception with error message.
     *
     * @param msg Error message.
     */
    public BinaryObjectException(String msg) {
        super(msg);
    }

    /**
     * Creates binary exception with {@link Throwable} as a cause.
     *
     * @param cause Cause.
     */
    public BinaryObjectException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates binary exception with error message and {@link Throwable} as a cause.
     *
     * @param msg Error message.
     * @param cause Cause.
     */
    public BinaryObjectException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}