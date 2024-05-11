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

package org.apache.ignite.ml.math.primitives.matrix.storage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.ignite.internal.util.collection.BitSetIntSet;
import org.apache.ignite.internal.util.collection.ImmutableIntSet;
import org.apache.ignite.internal.util.collection.IntHashMap;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.collection.IntSet;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.primitives.matrix.MatrixStorage;

/**
 * Storage for sparse, local, on-heap matrix.
 */
public class SparseMatrixStorage implements MatrixStorage, StorageConstants {
    /** Default zero value. */
    private static final double DEFAULT_VALUE = 0.0;

    /** */
    private int rows;

    /** */
    private int cols;   

    /** */
    private int stoMode;

    /** Actual map storage. */    
    private OpenMapRealMatrix sto;

    /** */
    public SparseMatrixStorage() {
        // No-op.
    }

    /** */
    public SparseMatrixStorage(int rows, int cols, int stoMode) {
        assert rows > 0;
        assert cols > 0;        
        assertStorageMode(stoMode);

        this.rows = rows;
        this.cols = cols;
        
        this.stoMode = stoMode;

        sto = new OpenMapRealMatrix(rows,cols);
    }

    /** {@inheritDoc} */
    @Override public int storageMode() {
        return stoMode;
    }

    /** {@inheritDoc} */
    @Override public int accessMode() {
        return stoMode;
    }

    /** {@inheritDoc} */
    @Override public double get(int x, int y) {
        if (stoMode == ROW_STORAGE_MODE) {
            double val = sto.getEntry(x,y);

            if (val != Double.NaN) {                
                return val;
            }

            return DEFAULT_VALUE;
        }
        else {
        	double val = sto.getEntry(x,y);

            if (val != Double.NaN) {                
                return val;
            }

            return DEFAULT_VALUE;
        }
    }

    /** {@inheritDoc} */
    @Override public void set(int x, int y, double v) {
        // Ignore default values (currently 0.0).
    	if (stoMode == ROW_STORAGE_MODE) {
            sto.setEntry(x, y, v);
        }
        else {
        	sto.setEntry(x, y, v);
        }
    }

    /** {@inheritDoc} */
    @Override public int columnSize() {
        return cols;
    }

    /** {@inheritDoc} */
    @Override public int rowSize() {
        return rows;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(rows);
        out.writeInt(cols);        
        out.writeInt(stoMode);
        out.writeObject(sto);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rows = in.readInt();
        cols = in.readInt();        
        stoMode = in.readInt();
        sto = (OpenMapRealMatrix)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return false;
    }

    // TODO: IGNITE-5777, optimize this

    /** {@inheritDoc} */
    @Override public double[] data() {
        double[] data = new double[rows * cols];

        boolean isRowStorage = stoMode == ROW_STORAGE_MODE;
        
        for (int i = 0; i < sto.getRowDimension(); ++i) {
            
            for (int j = 0; j < sto.getColumnDimension(); ++j) {
            	if (isRowStorage) {
            		int k = i * rows + j;
            		data[k] = sto.getEntry(i, j);
            	}
            	else{
                	int k = j * cols + i;
                    data[k] = sto.getEntry(i, j);
                }
            }
        }

        return data;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;
        res = res * 37 + sto.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        SparseMatrixStorage that = (SparseMatrixStorage)o;

        return rows == that.rows && cols == that.cols && stoMode == that.stoMode
            && (sto != null ? sto.equals(that.sto) : that.sto == null);
    }

    /** */
    public void compute(int row, int col, IgniteTriFunction<Integer, Integer, Double, Double> f) {
    	double val = sto.getEntry(row, col);
        sto.setEntry(row,col, f.apply(row, col, val));
    }

    public double[][] data2d() {
    	return sto.getData();
    }
    
    /** */
    public IntMap<IntSet> indexesMap() {
    	IntHashMap<IntSet> res = new IntHashMap<>();

        for (int row=0;row<rows;row++) {     
        	BitSetIntSet set = new BitSetIntSet(cols/4);
    		for(int j=0;j<cols;j++) {    			
    			if(sto.getEntry(row,j)!=0.0) {
    				set.add(j);
    			}
    		}
    		if(set.size()>0) {
    			res.put(row, set);
    		}    			
        }
        return res;
    }
}
