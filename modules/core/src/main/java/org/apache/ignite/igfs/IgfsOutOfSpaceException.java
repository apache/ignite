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

package org.apache.ignite.igfs;

import org.jetbrains.annotations.*;

/**
 * {@code GGFS} exception that is thrown when it detected out-of-space condition.
 * It is thrown when number of writes written to a {@code GGFS} data nodes exceeds
 * its maximum value (that is configured per-node).
 */
public class IgfsOutOfSpaceException extends IgfsException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates exception with given error message.
     *
     * @param msg Error message.
     */
    public IgfsOutOfSpaceException(String msg) {
        super(msg);
    }

    /**
     * Creates an instance of exception with given exception cause.
     *
     * @param cause Exception cause.
     */
    public IgfsOutOfSpaceException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates an instance of GGFS exception with given error message and given exception cause.
     *
     * @param msg Error message.
     * @param cause Exception cause.
     */
    public IgfsOutOfSpaceException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
