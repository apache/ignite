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

/**
 * File statistics aggregator.
 */
public class FileStat {
    /** Is directory. */
    private final boolean isDirectory;

    /** Modification time. */
    private final long modificationTime;

    /** Size. */
    private final int size;

    /**
     * Creates an instance of stat file.
     *
     * @param isDirectory Is directory.
     * @param modificationTime Modification time.
     * @param size Size.
     */
    public FileStat(boolean isDirectory, long modificationTime, int size) {
        this.isDirectory = isDirectory;
        this.modificationTime = modificationTime;
        this.size = size;
    }

    /**
     * @return True if file is directory.
     */
    public boolean isDirectory() {
        return isDirectory;
    }

    /**
     * @return Modification time.
     */
    public long getModificationTime() {
        return modificationTime;
    }

    /**
     * @return File size.
     */
    public int getSize() {
        return size;
    }
}
