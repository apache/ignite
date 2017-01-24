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
import org.apache.ignite.math.UnsupportedOperationException;
import org.apache.ignite.math.Vector;
import java.util.*;
import java.util.function.*;

/**
 * Basic implementation for matrix.
 * 
 * This is a trivial implementation for matrix assuming dense logic, local on-heap JVM storage
 * based on <code>double[][]</code> array. It is only suitable for data sets where
 * local, non-distributed execution is satisfactory and on-heap JVM storage is enough
 * to keep the entire data set.
 */
public class DenseLocalOnHeapMatrix implements Matrix {
    /** */
    private double[][] data;

    /**
     *
     */
    public DenseLocalOnHeapMatrix() {
        this(100, 100);
    }

    /**
     *
     * @param rows
     * @param cols
     */
    private void init(int rows, int cols) {
        data = new double[rows][cols];
    }

    /**
     *
     * @param mtx
     * @param shallowCopy
     */
    private void init(double[][] mtx, boolean shallowCopy) {
        data = shallowCopy ? mtx : mtx.clone();
    }

    /**
     *
     * @param rows
     * @param cols
     */
    public DenseLocalOnHeapMatrix(int rows, int cols) {
        init(rows, cols);
    }

    /**
     *
     * @param mtx
     * @param shallowCopy
     */
    public DenseLocalOnHeapMatrix(double[][] mtx, boolean shallowCopy) {
        init(mtx, shallowCopy);
    }

    /**
     *
     * @param args
     */
    public DenseLocalOnHeapMatrix(Map<String, Object> args) {
        if (args.containsKey("rows") && args.containsKey("cols"))
            init((int)args.get("rows"), (int)args.get("cols"));
        else if (args.containsKey("mtx") && args.containsKey("shallowCopy")) {
            init((double[][])args.get("mtx"), (boolean)args.get("shallowCopy"));
        }
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
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
    public Vector aggregateRows(Function<Vector, Double> fun) {
        return null; // TODO
    }

    @Override
    public Vector aggregateColumns(Function<Vector, Double> fun) {
        return null; // TODO
    }

    @Override
    public <T> T foldMap(BiFunction<T, Double, T> foldFun, DoubleFunction mapFun) {
        return null; // TODO
    }

    @Override
    public int columnSize() {
        return 0; // TODO
    }

    @Override
    public int rowSize() {
        return 0; // TODO
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
    public Matrix cloneEmpty() {
        return null; // TODO
    }

    @Override
    public Matrix clone() {
        return null; // TODO
    }

    @Override
    public Matrix cloneEmpty(int rows, int cols) {
        return null; // TODO
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
    public String flavor() {
        return null; // TODO
    }

    @Override
    public Optional<ClusterGroup> clusterGroup() {
        return null; // TODO
    }
}
