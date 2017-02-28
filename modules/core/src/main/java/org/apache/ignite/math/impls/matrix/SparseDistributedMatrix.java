// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

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

package org.apache.ignite.math.impls.matrix;

import org.apache.ignite.math.*;
import java.io.*;

/**
 * Sparse distributed matrix implementation based on data grid.
 */
public class SparseDistributedMatrix extends AbstractMatrix {
    /** Row-optimized storage mode. */
    public static final int ROW_STORAGE_MODE = 1;
    /** Column-optimized storage mode. */
    public static final int COLUMN_STORAGE_MODE = 2;
    /** Random-optimized access mode. */
    public static final int RANDOM_ACCESS_MODE = 3;
    /** Sequential-optimized access mode. */
    public static final int SEQUENTIAL_ACCESS_MODE = 4;

    private int rows, cols;
    private int stoMode, acsMode;

    /**
     *
     */
    public SparseDistributedMatrix() {
        // No-op.
    }

    /**
     *
     * @param rows
     * @param cols
     * @param stoMode
     * @param acsMode
     */
    public SparseDistributedMatrix(int rows, int cols, int stoMode, int acsMode) {
        assert rows > 0;
        assert cols > 0;
        assert stoMode == ROW_STORAGE_MODE || stoMode == COLUMN_STORAGE_MODE;
        assert acsMode == RANDOM_ACCESS_MODE || acsMode == SEQUENTIAL_ACCESS_MODE;

        this.rows = rows;
        this.cols = cols;
        this.stoMode = stoMode;
        this.acsMode = acsMode;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        // TODO
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        // TODO
    }

    @Override
    public Matrix copy() {
        return null; // TODO
    }

    @Override
    public Matrix like(int rows, int cols) {
        return null; // TODO
    }

    @Override
    public Vector likeVector(int crd) {
        return null; // TODO
    }
}
