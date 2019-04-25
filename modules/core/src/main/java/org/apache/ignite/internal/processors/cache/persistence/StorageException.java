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
