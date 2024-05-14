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
public class DenseFloatVectorStorage implements VectorStorage {
   
    /** Numeric vector array */
    private float[] data;

    /**
     * IMPL NOTE required by {@link Externalizable}.
     */
    public DenseFloatVectorStorage() {
        // No-op.
    }

    /**
     * @param size Vector size.
     */
    public DenseFloatVectorStorage(int size) {
        assert size >= 0;

        data = new float[size];
    }

    /**
     * @param data Backing data array.
     */
    public DenseFloatVectorStorage(float[] data) {
        assert data != null;

        this.data = data;
    }

   

    /** {@inheritDoc} */
    @Override public int size() {
    	if(data==null) return 0;
    	return data.length;            
    }

   

    /**
     * Tries to cast internal representation of data to array of Serializable objects if need.
     */
    private Serializable[] toGenericArray() {
    	Serializable[] rawData = new Serializable[data.length];
        if (data.length>0) {        	
            for (int i = 0; i < rawData.length; i++)
                rawData[i] = data[i];
            
        }
        return rawData;
    }

    /** {@inheritDoc} */
    @Override public double get(int i) {
    	return data[i];
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> T getRaw(int i) {        
        return (T) Float.valueOf(data[i]);
    }

    /** {@inheritDoc} */
    @Override public void set(int i, double v) {
    	data[i] = (float)v;
    }

    /** {@inheritDoc} */
    @Override public void setRaw(int i, Serializable v) {
        if(v instanceof Number)
        	this.data[i] = ((Number) v).floatValue();
        else if(v instanceof String)
        	this.data[i] = Float.parseFloat(v.toString());
        else
        	throw new ClassCastException("Vector only accept numeric values.");
    }

    /** {@inheritDoc}} */
    @Override public boolean isArrayBased() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public double[] data() {
        double[] data2 = new double[data.length];
        if (data.length>0) {            
            for (int i = 0; i < data.length; i++)
                data2[i] = data[i];
           
        }
        return data2;
    }

    /** {@inheritDoc} */
    @Override public Serializable[] rawData() {
    	return toGenericArray();        
    }
    
    public void clear() {
    	for (int i = 0; i < data.length; i++)
            data[i] = 0;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isNumeric() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        boolean isNull = data == null;
        out.writeBoolean(isNull);
        out.writeObject(data);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        boolean isNull = in.readBoolean();
        if(!isNull)
        	data = (float[])in.readObject();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DenseFloatVectorStorage storage = (DenseFloatVectorStorage)o;
        return Arrays.equals(data, storage.data);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = Arrays.hashCode(data);        
        return res;
    }
    
    public float[] getData() {
    	return data;
    }
}
