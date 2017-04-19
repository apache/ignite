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

import org.apache.ignite.ml.math.MatrixKeyMapper;

/** */
public class MatrixKeyMapperForTests implements MatrixKeyMapper<Integer> {
    /** */ private int rows;
    /** */ private int cols;

    /** */
    public MatrixKeyMapperForTests() {
        // No-op.
    }

    /** */
    public MatrixKeyMapperForTests(int rows, int cols) {
        this.rows = rows;
        this.cols = cols;
    }

    /** */
    @Override public Integer apply(int x, int y) {
        return x * cols + y;
    }

    /** */
    @Override public boolean isValid(Integer integer) {
        return (rows * cols) > integer;
    }

    /** */
    @Override public int hashCode() {
        int hash = 1;

        hash += hash * 31 + rows;
        hash += hash * 31 + cols;

        return hash;
    }

    /** */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        MatrixKeyMapperForTests that = (MatrixKeyMapperForTests)obj;

        return rows == that.rows && cols == that.cols;
    }
}
