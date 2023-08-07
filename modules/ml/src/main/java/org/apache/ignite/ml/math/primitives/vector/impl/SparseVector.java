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

package org.apache.ignite.ml.math.primitives.vector.impl;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;


import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.matrix.impl.SparseMatrix;
import org.apache.ignite.ml.math.primitives.vector.AbstractVector;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.Vector.Element;
import org.apache.ignite.ml.math.primitives.vector.storage.SparseVectorStorage;
import org.jetbrains.annotations.NotNull;


/**
 * Local on-heap sparse vector based on hash map storage.
 */
public class SparseVector extends AbstractVector implements StorageConstants {
    /**
     *
     */
    public SparseVector() {
        // No-op.
    }

    /**
     * @param map Underlying map.
     * @param cp Should given map be copied.
     */
    public SparseVector(Map<Integer, Double> map, boolean cp) {
        setStorage(new SparseVectorStorage(map, cp));
    }

    /**
     * @param size Vector size.
     */
    public SparseVector(int size) {
        setStorage(new SparseVectorStorage(size));
    }

    /** */
    private SparseVectorStorage storage() {
        return (SparseVectorStorage)getStorage();
    }

    /** {@inheritDoc} */
    @Override public Vector like(int crd) {
        return new SparseVector(crd);
    }

    /** {@inheritDoc} */
    @Override public Matrix likeMatrix(int rows, int cols) {
        return new SparseMatrix(rows, cols);
    }

    /** {@inheritDoc} */
    @Override public Vector times(double x) {
        if (x == 0.0)
            return this.assign(0);
        else
            return super.times(x);
    }
    
    /** {@inheritDoc} */
    @Override public Vector copy() {
    	SparseVector copy = new SparseVector(size());
    	int[] indexes = storage().indexes();
        int len = indexes.length;
        for (int i = 0; i < len; i++)
        	copy.set(indexes[i], this.get(indexes[i]));
        return copy;
    }
    
    /** {@inheritDoc} */
    @Override public Vector assign(Vector vec) {
        checkCardinality(vec);
        if(vec.isDense()) {
	        for (Vector.Element x : vec.all())
	            storageSet(x.index(), x.get());
        }
        else {
        	storage().clear();
        	for (Vector.Element x : vec.nonZeroes())
	            storageSet(x.index(), x.get());
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public double sum() {
        double sum = 0;
        int[] indexes = storage().indexes();
        int len = indexes.length;
        for (int i = 0; i < len; i++)
        	sum+=this.get(indexes[i]);
        return sum;
    }

    /** {@inheritDoc} */
    @Override public double dot(Vector vec) {
        checkCardinality(vec);
        double sum = 0.0;        
        int[] indexes = storage().indexes();
        int len = indexes.length;
        for (int i = 0; i < len; i++)
            sum += storageGet(indexes[i]) * vec.getX(indexes[i]);

        return sum;
    }

    /**
     * @return Result of dot with self.
     */
    protected double dotSelf() {
        double sum = 0.0;
        int len = size();
        
        for (Element elment : nonZeroes()) {
            double v = elment.get();
            sum += v * v;
        }

        return sum;
    }

    /** Indexes of non-default elements. */
    public int[] indexes() {
        return storage().indexes();
    }

    /** {@inheritDoc} */
    @Override public Spliterator<Double> nonZeroSpliterator() {
        return new Spliterator<Double>() {
            /** {@inheritDoc} */
            @Override public boolean tryAdvance(Consumer<? super Double> act) {
                int[] indexes = storage().indexes();

                for (int index : indexes)
                    act.accept(storageGet(index));

                return true;
            }

            /** {@inheritDoc} */
            @Override public Spliterator<Double> trySplit() {
                return null; // No Splitting.
            }

            /** {@inheritDoc} */
            @Override public long estimateSize() {
                return storage().indexes().length;
            }

            /** {@inheritDoc} */
            @Override public int characteristics() {
                return ORDERED | SIZED;
            }
        };
    }
    

    /** {@inheritDoc} */
    @Override public Iterable<Element> nonZeroes() {
        return new Iterable<Element>() {
            private int idx;
            private int idxNext = -1;
            private int[] indexes = storage().indexes();

            /** {@inheritDoc} */
            @NotNull
            @Override public Iterator<Element> iterator() {
                return new Iterator<Element>() {
                    @Override public boolean hasNext() {
                        findNext();

                        return !over();
                    }

                    @Override public Element next() {
                        if (hasNext()) {
                            idx = idxNext;

                            return getElement(indexes[idxNext]);
                        }

                        throw new NoSuchElementException();
                    }

                    private void findNext() {
                        if (over())
                            return;

                        if (idxNextInitialized() && idx != idxNext)
                            return;

                        if (idxNextInitialized())
                            idx = idxNext + 1;                        

                        idxNext = idx++;
                    }

                    private boolean over() {
                        return idxNext >= indexes.length;
                    }

                    private boolean idxNextInitialized() {
                        return idxNext != -1;
                    }
                };
            }
        };
    }
}
