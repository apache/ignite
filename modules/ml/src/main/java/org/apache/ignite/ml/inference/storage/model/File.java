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
 * Implementation of file {@link ModelStorageProvider} works with.
 */
class File implements FileOrDirectory {
    /** */
    private static final long serialVersionUID = -7739751667495712802L;

    /** File content. */
    private final byte[] data;

    /**
     * Constructs a new instance of file.
     *
     * @param data File content.
     */
    protected File(byte[] data) {
        this.data = data;
    }

    /** {@inheritDoc} */
    @Override public boolean isFile() {
        return true;
    }

    /** */
    protected byte[] getData() {
        return data;
    }
}
