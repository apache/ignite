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

package org.apache.ignite.fs;

import org.apache.ignite.*;
import org.jetbrains.annotations.*;

/**
 * {@code GGFS} exception thrown by file system components.
 */
public class IgniteFsException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates an instance of GGFS exception with descriptive error message.
     *
     * @param msg Error message.
     */
    public IgniteFsException(String msg) {
        super(msg);
    }

    /**
     * Creates an instance of GGFS exception caused by nested exception.
     *
     * @param cause Exception cause.
     */
    public IgniteFsException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates an instance of GGFS exception with error message and underlying cause.
     *
     * @param msg Error message.
     * @param cause Exception cause.
     */
    public IgniteFsException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
