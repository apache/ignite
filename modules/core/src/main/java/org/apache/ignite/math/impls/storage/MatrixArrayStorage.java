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

package org.apache.ignite.math.impls.storage;

import org.apache.ignite.math.*;
import java.io.*;
import java.util.*;

/**
 * TODO: add description.
 */
public class MatrixArrayStorage implements MatrixStorage {
    /** */
    private double[][] data;

    private int rows;

    private int cols;

    /**
     *
     */
    public MatrixArrayStorage() {
        this(null);
    }

    /**
     *
     * @param rows
     * @param cols
     */
    public MatrixArrayStorage(int rows, int cols) {
        this.data = new double[rows][cols];
        this.rows = rows;
        this.cols = cols;
    }

    /**
     * 
     * @param data
     */
    public MatrixArrayStorage(double[][] data) {
        this.data = data;
        this.rows = data.length;
        this.cols = data[0].length;
    }

    /** */
    @Override public boolean equals(Object o) {
        return this == o || !(o == null || getClass() != o.getClass())
            && Arrays.deepEquals(data, ((MatrixArrayStorage)o).data);
    }

    @Override
    public double get(int x, int y) {
        return data[x][y];
    }

    @Override
    public void set(int x, int y, double v) {
        data[x][y] = v;
    }

    @Override
    public int columnSize() {
        return cols;
    }

    @Override
    public int rowSize() {
        return rows;
    }

    @Override
    public boolean isArrayBased() {
        return true;
    }

    @Override
    public double[][] data() {
        return data;
    }

    /** {@inheritDoc */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(data);
    }

    /** {@inheritDoc */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        data = (double[][])in.readObject();
    }
}
