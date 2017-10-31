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

package org.apache.ignite.ml.math.impls.matrix;

import org.apache.ignite.ml.math.Matrix;

/**
 * Block for {@link SparseBlockDistributedMatrix}.
 */
public final class BlockEntry extends SparseLocalOnHeapMatrix {
    /** Max block size. */
    public static final int MAX_BLOCK_SIZE = 4;

    /** */
    public BlockEntry() {
        // No-op.
    }

    /** */
    public BlockEntry(int row, int col) {
        super(row, col);

        assert col <= MAX_BLOCK_SIZE;
        assert row <= MAX_BLOCK_SIZE;
    }

    /** */
    public BlockEntry(Matrix mtx) {
        assert mtx.columnSize() <= MAX_BLOCK_SIZE;
        assert mtx.rowSize() <= MAX_BLOCK_SIZE;

        setStorage(mtx.getStorage());
    }

}
