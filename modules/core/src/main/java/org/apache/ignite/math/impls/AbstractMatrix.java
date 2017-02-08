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

import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.math.*;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.storage.*;
import java.io.*;
import java.util.*;
import java.util.function.*;

/**
 * TODO: add description.
 */
public abstract class AbstractMatrix implements Matrix, Externalizable {
    /** Matrix storage implementation. */
    private MatrixStorage sto;

    /** Matrix's GUID. */
    private IgniteUuid guid = IgniteUuid.randomUuid();

    /**
     *
     * @param sto
     */
    public AbstractMatrix(MatrixStorage sto) {
        this.sto = sto;
    }

    /**
     *
     */
    public AbstractMatrix() {
        sto = new MatrixNullStorage();
    }

    /**
     *
     * @param sto
     */
    protected void setStorage(MatrixStorage sto) {
        this.sto = sto == null ? new MatrixNullStorage() : sto;
    }

    /**
     *
     * @param x
     * @param y
     * @param v
     */
    protected void storageSet(int x, int y, double v) {
        sto.set(x, y, v);
    }

    /**
     *
     * @param x
     * @param y
     * @return
     */
    protected double storageGet(int x, int y) {
        return sto.get(x, y);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        // TODO
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // TODO
    }

    @Override
    public Matrix assign(double val) {
        return null; // TODO
    }

    @Override
    public Matrix assign(double[][] vals) {
        return null; // TODO
    }

    @Override
    public Matrix assign(Matrix mtx) {
        return null; // TODO
    }

    @Override
    public Matrix map(DoubleFunction<Double> fun) {
        return null; // TODO
    }

    @Override
    public Matrix map(Matrix mtx, BiFunction<Double, Double, Double> fun) {
        return null; // TODO
    }

    @Override
    public Matrix assignColumn(int col, Vector vec) {
        return null; // TODO
    }

    @Override
    public Matrix assignRow(int row, Vector vec) {
        return null; // TODO
    }

    @Override
    public Vector foldRows(Function<Vector, Double> fun) {
        return null; // TODO
    }

    @Override
    public Vector foldColumns(Function<Vector, Double> fun) {
        return null; // TODO
    }

    @Override
    public <T> T foldMap(BiFunction<T, Double, T> foldFun, DoubleFunction mapFun, T zeroVal) {
        return null; // TODO
    }

    @Override
    public int columnSize() {
        return sto.columnSize();
    }

    @Override
    public int rowSize() {
        return sto.rowSize();
    }

    @Override
    public double determinant() {
        return 0; // TODO
    }

    @Override
    public Matrix divide(double x) {
        return null; // TODO
    }

    @Override
    public double get(int row, int col) {
        return 0; // TODO
    }

    @Override
    public double getX(int row, int col) {
        return 0; // TODO
    }

    @Override
    public Matrix minus(Matrix mtx) {
        return null; // TODO
    }

    @Override
    public Matrix plus(double x) {
        return null; // TODO
    }

    @Override
    public Matrix plus(Matrix mtx) {
        return null; // TODO
    }

    @Override
    public IgniteUuid guid() {
        return null; // TODO
    }

    @Override
    public Matrix set(int row, int col, double val) {
        return null; // TODO
    }

    @Override
    public Matrix setRow(int row, double[] data) {
        return null; // TODO
    }

    @Override
    public Matrix setColumn(int col, double[] data) {
        return null; // TODO
    }

    @Override
    public Matrix setX(int row, int col, double val) {
        return null; // TODO
    }

    @Override
    public Matrix times(double x) {
        return null; // TODO
    }

    @Override
    public Matrix times(Matrix mtx) {
        return null; // TODO
    }

    @Override
    public double sum() {
        return 0; // TODO
    }

    @Override
    public Matrix transpose() {
        return null; // TODO
    }

    @Override
    public Matrix viewPart(int[] offset, int[] size) {
        return null; // TODO
    }

    @Override
    public Vector viewRow(int row) {
        return null; // TODO
    }

    @Override
    public Vector viewColumn(int col) {
        return null; // TODO
    }

    @Override
    public Vector viewDiagonal() {
        return null; // TODO
    }

    @Override
    public Optional<ClusterGroup> clusterGroup() {
        return null; // TODO
    }
}
