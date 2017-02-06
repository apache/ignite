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

import java.io.*;
import java.util.*;
import java.util.function.*;

/**
 * Basic implementation for vector.
 * <p>
 * This is a trivial implementation for vector assuming dense logic, local on-heap JVM storage
 * based on <code>double[]</code> array. It is only suitable for data sets where
 * local, non-distributed execution is satisfactory and on-heap JVM storage is enough
 * to keep the entire data set.
 */
public class DenseLocalOnHeapVector implements Vector, Externalizable {
    /** */
    private final double[] data;

    /** */
    private final int DFLT_SIZE = 100;

    /**
     * @param size Vector cardinality.
     */
    private double[] init(int size) {
        return new double[size];
    }

    /**
     * @param arr
     * @param shallowCp
     */
    private double[] init(double[] arr, boolean shallowCp) {
        return shallowCp ? arr : arr.clone();
    }

    /**
     * @param args
     */
    public DenseLocalOnHeapVector(Map<String, Object> args) {
        if (args == null)
            data = init(DFLT_SIZE);
        else if (args.containsKey("size"))
            data = init((int) args.get("size"));
        else if (args.containsKey("arr") && args.containsKey("shallowCopy"))
            data = init((double[]) args.get("arr"), (boolean) args.get("shallowCopy"));
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
    }

    /** */
    public DenseLocalOnHeapVector() {
        data = init(DFLT_SIZE);
    }

    /**
     * @param size Vector cardinality.
     */
    public DenseLocalOnHeapVector(int size) {
        data = init(size);
    }

    /**
     * @param arr
     * @param shallowCp
     */
    public DenseLocalOnHeapVector(double[] arr, boolean shallowCp) {
        data = init(arr, shallowCp);
    }

    /**
     * @param arr
     */
    public DenseLocalOnHeapVector(double[] arr) {
        this(arr, false);
    }

    /** IMPL NOTE for clone, see eg http://www.agiledeveloper.com/articles/cloning072002.htm */
    private DenseLocalOnHeapVector(DenseLocalOnHeapVector original) {
        this.data = original.data == null ? null : original.data.clone();
    }

    /** {@inheritDoc */
    @Override public int size() {
        return data == null ? 0 : data.length;
    }

    /** {@inheritDoc */
    @Override public boolean isDense() {
        return true;
    }

    /** {@inheritDoc */
    @Override public boolean isSequentialAccess() {
        return true;
    }

    /** */
    @Override public boolean equals(Object o) {
        return this == o || !(o == null || getClass() != o.getClass())
            && Arrays.equals(data, ((DenseLocalOnHeapVector) o).data);
    }

    /** */
    @Override public int hashCode() {
        return Arrays.hashCode(data);
    }

    /** {@inheritDoc */
    @Override public DenseLocalOnHeapVector clone() {
        try {
            return new DenseLocalOnHeapVector((DenseLocalOnHeapVector) super.clone());
        }
        catch (CloneNotSupportedException e) {
            return new DenseLocalOnHeapVector(this);
        }
    }

    /** {@inheritDoc */
    @Override public Iterable<Element> all() {
        return new Iterable<Element>() {
            private int idx = 0;

            @Override public Iterator<Element> iterator() {
                return new Iterator<Element>() {
                    @Override public boolean hasNext() {
                        return idx < size();
                    }

                    @Override public Element next() {
                        if (hasNext())
                            return getElement(idx++);

                        throw new NoSuchElementException();
                    }
                };
            }
        };
    }

    /** {@inheritDoc */
    @Override public Iterable<Element> nonZeroes() {
        return new Iterable<Element>() {
            private final static int NOT_INITIALIZED = -1;

            private int idx = 0;

            private int idxNext = NOT_INITIALIZED;

            @Override public Iterator<Element> iterator() {
                return new Iterator<Element>() {
                    @Override public boolean hasNext() {
                        findNext();

                        return !over();
                    }

                    @Override public Element next() {
                        if (hasNext()) {
                            idx = idxNext;

                            return getElement(idxNext);
                        }

                        throw new NoSuchElementException();
                    }

                    private void findNext() {
                        if (over())
                            return;

                        if (idxNext != NOT_INITIALIZED && idx != idxNext)
                            return;

                        while (idx < size() && zero(get(idx)))
                            idx++;

                        idxNext = idx++;
                    }

                    private boolean over() {
                        return idxNext >= size();
                    }
                };
            }
        };
    }

    /** */
    private boolean zero(double val) {
        return val == 0.0;
    }

    /** {@inheritDoc */
    @Override public Element getElement(int idx) {
        return new Element() {
            @Override public double get() {
                return data[idx];
            }

            @Override public int index() {
                return idx;
            }

            @Override public void set(double val) {
                data[idx] = val;
            }
        };
    }

    /** {@inheritDoc */
    @Override public Spliterator<Double> allSpliterator() {
        return Arrays.spliterator(data);
    }

    /** {@inheritDoc */
    @Override public Spliterator<Double> nonZeroSpliterator() {
        return null; // TODO
    }

    /** {@inheritDoc */
    @Override public Vector assign(double val) {
        Arrays.fill(data, val);

        return this;
    }

    /** {@inheritDoc */
    @Override public Vector assign(double[] vals) {
        if (vals.length != data.length)
            throw new CardinalityException(data.length, vals.length);

        System.arraycopy(vals, 0, data, 0, vals.length);

        return this;
    }

    /** {@inheritDoc */
    @Override public Vector assign(IntToDoubleFunction fun) {
        assert fun != null;

        Arrays.setAll(data, fun);

        return this;
    }

    /** {@inheritDoc */
    @Override public Vector assign(Vector vec) {
        assert vec != null;

        if (vec.size() != size())
            throw new CardinalityException(size(), vec.size());


        for (Vector.Element x : vec.all())
            data[x.index()] = x.get();

        return this;
    }

    /** {@inheritDoc */
    @Override public Vector map(DoubleFunction<Double> fun) {
        assert fun != null;

        Arrays.setAll(data, (idx) -> fun.apply(data[idx]));

        return this;
    }

    /** {@inheritDoc */
    @Override public Vector map(Vector vec, DoubleFunction<Double> fun) {
        return null; // TODO
    }

    /** {@inheritDoc */
    @Override public Vector map(BiFunction<Double, Double, Double> fun, double y) {
        return null; // TODO
    }

    /** {@inheritDoc */
    @Override public Vector divide(double x) {
        return null; // TODO
    }

    /** {@inheritDoc */
    @Override public double dot(Vector vec) {
        return 0; // TODO
    }

    /** {@inheritDoc */
    @Override public double get(int idx) {
        return 0; // TODO
    }

    /** {@inheritDoc */
    @Override public double getX(int idx) {
        return 0; // TODO
    }

    /** {@inheritDoc */
    @Override public Vector like(int crd) {
        return null; // TODO
    }

    /** {@inheritDoc */
    @Override public Vector minus(Vector vec) {
        return null; // TODO
    }

    /** {@inheritDoc */
    @Override public Vector normalize() {
        return null; // TODO
    }

    /** {@inheritDoc */
    @Override public Vector normalize(double power) {
        return null; // TODO
    }

    /** {@inheritDoc */
    @Override public Vector logNormalize() {
        return null; // TODO
    }

    /** {@inheritDoc */
    @Override public Vector logNormalize(double power) {
        return null; // TODO
    }

    /** {@inheritDoc */
    @Override public double norm(double power) {
        return 0; // TODO
    }

    /** {@inheritDoc */
    @Override public Element minValue() {
        return null; // TODO
    }

    /** {@inheritDoc */
    @Override public double maxValue() {
        return 0; // TODO
    }

    /** {@inheritDoc */
    @Override public Vector plus(double x) {
        return null; // TODO
    }

    /** {@inheritDoc */
    @Override public Vector plus(Vector vec) {
        return null; // TODO
    }

    /** {@inheritDoc */
    @Override public Vector set(int idx, double val) {
        return null; // TODO
    }

    /** {@inheritDoc */
    @Override public Vector setX(int idx, double val) {
        return null; // TODO
    }

    /** {@inheritDoc */
    @Override public void incrementX(int idx, double val) {
        // TODO
    }

    /** {@inheritDoc */
    @Override public int nonDefaultElements() {
        return 0; // TODO
    }

    /** {@inheritDoc */
    @Override public int nonZeroElements() {
        int res = 0;

        for (Element ignored : nonZeroes())
            res++;

        return res;
    }

    /** {@inheritDoc */
    @Override public Vector times(double x) {
        return null; // TODO
    }

    /** {@inheritDoc */
    @Override public Vector times(Vector x) {
        return null; // TODO
    }

    /** {@inheritDoc */
    @Override public Vector viewPart(int off, int len) {
        return null; // TODO
    }

    /** {@inheritDoc */
    @Override public double sum() {
        return 0; // TODO
    }

    /** {@inheritDoc */
    @Override public Matrix cross(Vector vec) {
        return null; // TODO
    }

    /** {@inheritDoc */
    @Override public <T> T foldMap(BiFunction<T, Double, T> foldFun, DoubleFunction mapFun) {
        return null; // TODO
    }

    /** {@inheritDoc */
    @Override public double getLengthSquared() {
        return 0; // TODO
    }

    /** {@inheritDoc */
    @Override public double getDistanceSquared(Vector vec) {
        return 0; // TODO
    }

    /** {@inheritDoc */
    @Override public double getLookupCost() {
        return 0; // TODO
    }

    /** {@inheritDoc */
    @Override public boolean isAddConstantTime() {
        return true;
    }

    /** {@inheritDoc */
    @Override public Optional<ClusterGroup> clusterGroup() {
        return null; // TODO
    }

    /** {@inheritDoc */
    @Override public IgniteUuid guid() {
        return null; // TODO
    }

    /** {@inheritDoc */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // TODO
    }

    /** {@inheritDoc */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // TODO
    }
}
