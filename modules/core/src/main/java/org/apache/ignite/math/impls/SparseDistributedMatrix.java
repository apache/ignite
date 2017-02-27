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

package org.apache.ignite.math.impls;

import org.apache.ignite.configuration.*;
import org.apache.ignite.math.*;

/**
 * Sparse distributed matrix implementation based on data grid.
 */
public class SparseDistributedMatrix extends AbstractMatrix {
    private int rows, cols;
    private boolean rowWise, rndAccess;

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
     * @param rowWise Optimized for row or column operations. In other words, the matrix will be stored in
     *      a cache of rows or a cache of columns where each row or column will be represented as
     *      sparse local on-heap vector.
     * @param rdnAccess Sequential or random access optimization for row or column vectors.
     */
    public SparseDistributedMatrix(int rows, int cols, boolean rowWise, boolean rdnAccess) {
        this.rows = rows;
        this.cols = cols;
        this.rowWise = rowWise;
        this.rndAccess = rndAccess;
    }

    /**
     *
     * @return
     */
    public boolean isRowWise() {
        return rowWise;
    }

    /**
     *
     * @return
     */
    public boolean isColumnWise() {
        return !rowWise;
    }

    /**
     *
     * @return
     */
    public boolean isRandomAccess() {
        return rndAccess;
    }

    /**
     * 
     * @return
     */
    public boolean isColumnAccess() {
        return !rndAccess;
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
