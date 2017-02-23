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

import org.apache.ignite.math.*;
import org.apache.ignite.math.impls.storage.*;
import java.util.function.*;

/**
 * Abstract matrix optimized for cache-based operations. It expects the {@link CacheMatrixStorage}
 * storage.
 *
 * @see CacheMatrixStorage
 */
public abstract class CacheAbstractMatrix extends AbstractMatrix {
    /**
     * Gets cache-based storage.
     * 
     * @return
     */
    protected CacheMatrixStorage storage() {
        return (CacheMatrixStorage)getStorage();
    }

    @Override
    public Matrix swapRows(int row1, int row2) {
        return super.swapRows(row1, row2); // TODO
    }

    @Override
    public Matrix assign(double val) {
        return super.assign(val); // TODO
    }

    @Override
    public Matrix assign(double[][] vals) {
        return super.assign(vals); // TODO
    }

    @Override
    public Matrix assign(Matrix mtx) {
        return super.assign(mtx); // TODO
    }

    @Override
    public Matrix map(DoubleFunction<Double> fun) {
        return super.map(fun); // TODO
    }

    @Override
    public Matrix map(Matrix mtx, BiFunction<Double, Double, Double> fun) {
        return super.map(mtx, fun); // TODO
    }

    @Override
    public Matrix assignColumn(int col, Vector vec) {
        return super.assignColumn(col, vec); // TODO
    }

    @Override
    public Matrix assignRow(int row, Vector vec) {
        return super.assignRow(row, vec); // TODO
    }

    @Override
    public Vector foldRows(Function<Vector, Double> fun) {
        return super.foldRows(fun); // TODO
    }

    @Override
    public Vector foldColumns(Function<Vector, Double> fun) {
        return super.foldColumns(fun); // TODO
    }

    @Override
    public Matrix divide(double d) {
        return super.divide(d); // TODO
    }

    @Override
    public Matrix minus(Matrix mtx) {
        return super.minus(mtx); // TODO
    }

    @Override
    public Matrix plus(double x) {
        return super.plus(x); // TODO
    }

    @Override
    public Matrix plus(Matrix mtx) {
        return super.plus(mtx); // TODO
    }

    @Override
    public Matrix setRow(int row, double[] data) {
        return super.setRow(row, data); // TODO
    }

    @Override
    public Matrix setColumn(int col, double[] data) {
        return super.setColumn(col, data); // TODO
    }

    @Override
    public Matrix times(double x) {
        return super.times(x); // TODO
    }

    @Override
    public double sum() {
        return super.sum(); // TODO
    }
}
