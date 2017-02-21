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
import org.apache.ignite.math.UnsupportedOperationException;
import java.io.*;

/**
 * TODO: add description.
 */
public class DiagonalMatrixStorage implements MatrixStorage {
    private Vector diagonal;

    /**
     *
     */
    public DiagonalMatrixStorage() {
        // No-op.
    }

    /**
     *
     * @param diagonal
     */
    public DiagonalMatrixStorage(Vector diagonal) {
        this.diagonal = diagonal;
    }

    /**
     * 
     * @return
     */
    public Vector diagonal() {
        return diagonal;
    }

    @Override
    public double get(int x, int y) {
        return x == y ? diagonal.get(x) : 0.0;
    }

    @Override
    public void set(int x, int y, double v) {
        if (x == y)
            diagonal.set(x, v);
        else
            throw new UnsupportedOperationException("Can't set off-diagonal element.");
    }

    @Override
    public int columnSize() {
        return diagonal.size();
    }

    @Override
    public int rowSize() {
        return diagonal.size();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
       out.writeObject(diagonal);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        diagonal = (Vector)in.readObject();
    }

    @Override
    public boolean isSequentialAccess() {
        return diagonal.isSequentialAccess();
    }

    @Override
    public boolean isDense() {
        return diagonal.isDense();
    }

    @Override
    public double getLookupCost() {
        return diagonal.getLookupCost();
    }

    @Override
    public boolean isAddConstantTime() {
        return diagonal.isAddConstantTime();
    }

    @Override
    public boolean isArrayBased() {
        return false;
    }
}
