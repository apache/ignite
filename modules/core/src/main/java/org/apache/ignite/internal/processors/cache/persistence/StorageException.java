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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.IOException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.InvalidEnvironmentException;
import org.jetbrains.annotations.NotNull;

/**
 * Exception is needed to distinguish WAL manager & page store critical I/O errors.
 */
public class StorageException extends IgniteCheckedException implements InvalidEnvironmentException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param msg Error message.
     * @param cause Error cause.
     */
    public StorageException(String msg, @NotNull IOException cause) {
        super(msg, cause);
    }

    /**
     * @param e Cause exception.
     */
    public StorageException(IOException e) {
        super(e);
    }

    /**
     * @param msg Error message
     */
    public StorageException(String msg) {
        super(msg);
    }
}
