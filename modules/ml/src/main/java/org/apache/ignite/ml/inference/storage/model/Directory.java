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

import java.util.HashSet;
import java.util.Set;

/**
 * Implementation of directory {@link ModelStorageProvider} works with.
 */
class Directory implements FileOrDirectory {
    /** */
    private static final long serialVersionUID = -6441963559954107245L;

    /** Time since last modification. */
    private final long modificationTs;

    /** List of files in the directory. */
    private final Set<String> files = new HashSet<>();

    /**
     * Creates an instance of class.
     */
    public Directory() {
        this.modificationTs = System.currentTimeMillis();
    }

    /**
     * Creates an instance of class.
     *
     * @param modificationTs Modification time.
     */
    public Directory(long modificationTs) {
        this.modificationTs = modificationTs;
    }

    /** {@inheritDoc} */
    @Override public boolean isFile() {
        return false;
    }

    /** */
    Set<String> getFiles() {
        return files;
    }

    /** {@inheritDoc} */
    @Override public long getModificationTs() {
        return modificationTs;
    }

    /** {@inheritDoc} */
    @Override public FileOrDirectory updateModifictaionTs(long modificationTs) {
        Directory directory = new Directory(modificationTs);
        directory.getFiles().addAll(this.files);
        return directory;
    }
}
