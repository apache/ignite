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

import org.jetbrains.annotations.Nullable;

/**
 * Exception indicating that path is directory, while it is expected to be a file.
 */
public class IgfsPathIsDirectoryException extends IgfsException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Constructor.
     *
     * @param msg Message.
     */
    public IgfsPathIsDirectoryException(String msg) {
        super(msg);
    }

    /**
     * Constructor.
     *
     * @param cause Cause.
     */
    public IgfsPathIsDirectoryException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructor.
     *
     * @param msg   Message.
     * @param cause Cause.
     */
    public IgfsPathIsDirectoryException(@Nullable String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}