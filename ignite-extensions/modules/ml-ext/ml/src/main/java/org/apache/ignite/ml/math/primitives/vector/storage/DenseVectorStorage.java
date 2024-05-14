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

package org.apache.ignite.ml.math.primitives.vector.storage;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Arrays;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.math.primitives.vector.VectorStorage;

/**
 * Array based {@link VectorStorage} implementation.
 */
public class DenseVectorStorage implements VectorStorage {
    /** Raw data array. */
    private Serializable[] rawData;

    /**
     * IMPL NOTE required by {@link Externalizable}.
     */
    public DenseVectorStorage() {
        // No-op.
    }

   
    public DenseVectorStorage(int size) {
    	 this.rawData = new Serializable[size];    	 
    }

    /**
     * @param data Backing data array.
     */
    public DenseVectorStorage(Serializable[] data) {
        assert data != null;

        this.rawData = data;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        if (rawData == null)
            return 0;
        else
            return rawData.length;
    }

    /**
     * Tries to cast internal representation of data to array of doubles if need.
     */
    private double[] toNumericArray() {    	
        if (rawData == null)
            return null;
        double[] data = new double[rawData.length];
        if (rawData.length>0) {            
            for (int i = 0; i < rawData.length; i++)
                data[i] = rawData[i] == null ? 0.0 : ((Number)rawData[i]).doubleValue(); //TODO: IGNITE-11664
           
        }
        return data;
    }
    

    /** {@inheritDoc} */
    @Override public double get(int i) {       

        Serializable v = rawData[i];
        //TODO: IGNITE-11664 
    	if(v instanceof String)
    		return Double.parseDouble(v.toString());
    	
        return v == null ? 0.0 : ((Number)rawData[i]).doubleValue();
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> T getRaw(int i) {
        return (T)rawData[i];
    }

    /** {@inheritDoc} */
    @Override public void set(int i, double v) {      
        rawData[i] = v;
    }

    /** {@inheritDoc} */
    @Override public void setRaw(int i, Serializable v) {
    	this.rawData[i] = v;
    }

    /** {@inheritDoc}} */
    @Override public boolean isArrayBased() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public double[] data() {
        if (!isNumeric())
            throw new ClassCastException("Vector has not only numeric values.");
        
        return toNumericArray();
    }

    /** {@inheritDoc} */
    @Override public Serializable[] rawData() {        
        return rawData;
    }
    
    public void clear() {
    	for (int i = 0; i < rawData.length; i++)
    		rawData[i] = null;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isNumeric() {
        if (rawData == null)
            return true;

        for (int i = 0; i < rawData.length; i++) {
            if (rawData[i] != null && !(rawData[i] instanceof Number))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        boolean isNull = rawData == null;
        out.writeBoolean(isNull);
        if (!isNull)
            out.writeObject(rawData);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        boolean isNull = in.readBoolean();
        if (!isNull) {
            rawData = (Serializable[])in.readObject();            
        }       
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DenseVectorStorage storage = (DenseVectorStorage)o;
        return Arrays.equals(rawData, storage.rawData);
            
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = Arrays.hashCode(rawData);        
        return res;
    }
    
    public Serializable[] getData() {
    	return rawData;
    }
   
}
