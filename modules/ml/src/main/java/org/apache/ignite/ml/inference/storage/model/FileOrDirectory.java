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

package org.apache.ignite.ml.inference.storage.model;

import java.io.Serializable;

/**
 * Base interface for file or directory {@link ModelStorageProvider} works with.
 */
public interface FileOrDirectory extends Serializable {
    /**
     * Returns {@code true} if this object is a regular file, otherwise {@code false}.
     *
     * @return {@code true} if this object is a regular file, otherwise {@code false}.
     */
    public boolean isFile();

    /**
     * Return {@code true} if this object is a directory, otherwise {@code false}.
     *
     * @return {@code true} if this object is a directory, otherwise {@code false}.
     */
    public default boolean isDirectory() {
        return !isFile();
    }

    /**
     * Returns time since file modification.
     *
     * @return time since file modification.
     */
    public long getModificationTs();

    /**
     * Create new instance of filesystem object with modified timestamp.
     *
     * @param modificationTs Modification timestamp.
     * @return new instance with new value.
     */
    public FileOrDirectory updateModifictaionTs(long modificationTs);

    /**
     * Create new instance of filesystem object with current timestamp.
     */
    public default FileOrDirectory updateModifictaionTs() {
        return updateModifictaionTs(System.currentTimeMillis());
    }
}
