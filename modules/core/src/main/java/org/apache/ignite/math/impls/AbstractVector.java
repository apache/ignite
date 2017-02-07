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
import java.io.*;
import java.util.*;
import java.util.function.*;

/**
 * TODO: add description.
 */
public abstract class AbstractVector implements Vector, Externalizable {
    // Vector storage implementation.
    private VectorStorage sto;

    // Vector's GUID.
    private IgniteUuid guid = IgniteUuid.randomUuid();

    /**
     *
     * @param sto
     */
    public AbstractVector(VectorStorage sto) {
        this.sto = sto;
    }

    /**
     *
     */
    public AbstractVector() {
    }

    /**
     *
     * @param sto
     */
    protected void setStorage(VectorStorage sto) {
        this.sto = sto;
    }

    @Override
    public int size() {
        return sto.size();
    }

    /**
     *
     * @param idx
     */
    private void checkIndex(int idx) {
        if (idx < 0 || idx >= sto.size())
            throw new IndexException(idx);
    }

    @Override
    public double get(int idx) {
        checkIndex(idx);

        return sto.get(idx);
    }

    @Override
    public double getX(int idx) {
        return sto.get(idx);
    }

    @Override
    public Vector map(DoubleFunction<Double> fun) {
        if (sto.isArrayBased()) {
            double[] data = sto.data();

            Arrays.setAll(data, (idx) -> fun.apply(data[idx]));
        }
        else {
            int len = sto.size();

            for (int i = 1; i < len; i++)
                sto.set(i, fun.apply(sto.get(i)));
        }

        return this;
    }

    @Override
    public Vector map(Vector vec, BiFunction<Double, Double, Double> fun) {
        if (vec.size() != size())
            throw new CardinalityException(size(), vec.size());

        int len = sto.size();

        for (int i = 1; i < len; i++)
            sto.set(i, fun.apply(vec.get(i), sto.get(i)));

        return this;
    }

    @Override
    public Vector map(BiFunction<Double, Double, Double> fun, double y) {
        int len = sto.size();

        for (int i = 1; i < len; i++)
            sto.set(i, fun.apply(sto.get(i), y));

        return this;
    }

    /**
     *
     * @param idx
     * @return
     */
    private Element mkElement(int idx) {
        return new Element() {
            @Override
            public double get() {
                return sto.get(idx);
            }

            @Override
            public int index() {
                return idx;
            }

            @Override
            public void set(double value) {
                sto.set(idx, value);
            }
        };
    }

    @Override
    public Element minValue() {
        int minIdx = 0;
        int len = sto.size();

        for (int i = 1; i < len; i++)
            if (sto.get(i) < sto.get(minIdx))
                minIdx = i;

        return mkElement(minIdx);
    }

    @Override
    public Element maxValue() {
        int maxIdx = 0;
        int len = sto.size();

        for (int i = 1; i < len; i++)
            if (sto.get(i) > sto.get(maxIdx))
                maxIdx = i;

        return mkElement(maxIdx);
    }

    @Override
    public Vector set(int idx, double val) {
        checkIndex(idx);

        sto.set(idx, val);

        return this;
    }

    @Override
    public Vector setX(int idx, double val) {
        sto.set(idx, val);

        return this;
    }

    @Override
    public Vector increment(int idx, double val) {
        checkIndex(idx);
        
        sto.set(idx, sto.get(idx) + val);

        return this;
    }

    @Override
    public Vector incrementX(int idx, double val) {
        sto.set(idx, sto.get(idx) + val);
        
        return this;
    }

    /**
     * Tests if given value is considered a zero value.
     *
     * @param val Value to check.
     */
    protected boolean isZero(double val) {
        return val == 0.0;
    }

    @Override
    public double sum() {
        int sum = 0;
        int len = sto.size();

        for (int i = 1; i < len; i++)
            sum += sto.get(i);

        return sum;
    }

    @Override
    public IgniteUuid guid() {
        return guid;
    }

    @Override
    public int hashCode() {
        return guid.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return this == o || !(o == null || getClass() != o.getClass()) && sto.equals(((AbstractVector)o).sto);
    }

    @Override
    public Iterable<Element> all() {
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

    @Override public int nonZeroElements() {
        int cnt = 0;

        for (Element ignored : nonZeroes())
            cnt++;

        return cnt;
    }

    @Override public Optional<ClusterGroup> clusterGroup() {
        return null;
    }

    @Override
    public <T> T foldMap(BiFunction<T, Double, T> foldFun, DoubleFunction<Double> mapFun) {
        T t = null;
        int len = sto.size();

        for (int i = 1; i < len; i++)
            t = foldFun.apply(t, mapFun.apply(sto.get(i)));

        return t;
    }

    @Override
    public <T> T foldMap(Vector vec, BiFunction<T, Double, T> foldFun, BiFunction<Double, Double, Double> combFun) {
        if (vec.size() != sto.size())
            throw new CardinalityException(sto.size(), vec.size());

        T t = null;
        int len = sto.size();

        for (int i = 1; i < len; i++)
            t = foldFun.apply(t, combFun.apply(sto.get(i), vec.getX(i)));

        return t;
    }

    @Override public double norm(double power) {
        return 0; // TODO
    }

    @Override
    public Iterable<Element> nonZeroes() {
        return new Iterable<Element>() {
            private int idx = 0;
            private int idxNext = -1;

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

                        if (idxNext != -1 && idx != idxNext)
                            return;

                        while (idx < size() && isZero(get(idx)))
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

    @Override
    public Vector assign(double val) {
        if (sto.isArrayBased())
            Arrays.fill(sto.data(), val);
        else {
            int len = sto.size();

            for (int i = 0; i < len; i++)
                sto.set(i, val);
        }

        return this;
    }

    @Override
    public Vector assign(double[] vals) {
        if (vals.length != sto.size())
            throw new CardinalityException(sto.size(), vals.length);

        if (sto.isArrayBased())
            System.arraycopy(vals, 0, sto.data(), 0, vals.length);
        else {
            int len = sto.size();

            for (int i = 0; i < len; i++)
                sto.set(i, vals[i]);
        }

        return this;
    }

    @Override
    public Vector assign(Vector vec) {
        assert vec != null;

        if (vec.size() != sto.size())
            throw new CardinalityException(sto.size(), vec.size());

        for (Vector.Element x : vec.all())
            sto.set(x.index(), x.get());

        return this;
    }

    @Override
    public Vector assign(IntToDoubleFunction fun) {
        assert fun != null;

        if (sto.isArrayBased())
            Arrays.setAll(sto.data(), fun);
        else {
            int len = sto.size();

            for (int i = 0; i < len; i++)
                sto.set(i, fun.applyAsDouble(i));
        }

        return this;
    }

    @Override
    public Spliterator<Double> allSpliterator() {
        return new Spliterator<Double>() {
            @Override
            public boolean tryAdvance(Consumer<? super Double> action) {
                int len = sto.size();

                for (int i = 0; i < len; i++)
                    action.accept(sto.get(i));

                return true;
            }

            @Override
            public Spliterator<Double> trySplit() {
                return null; // No Splitting.
            }

            @Override
            public long estimateSize() {
                return sto.size();
            }

            @Override
            public int characteristics() {
                return ORDERED | SIZED;
            }
        };
    }

    @Override
    public Spliterator<Double> nonZeroSpliterator() {
        return new Spliterator<Double>() {
            @Override
            public boolean tryAdvance(Consumer<? super Double> action) {
                int len = sto.size();

                for (int i = 0; i < len; i++) {
                    double val = sto.get(i);

                    if (!isZero(val))
                        action.accept(val);
                }

                return true;
            }

            @Override
            public Spliterator<Double> trySplit() {
                return null; // No Splitting.
            }

            @Override
            public long estimateSize() {
                return nonZeroElements();
            }

            @Override
            public int characteristics() {
                return ORDERED | SIZED;
            }
        };
    }

    @Override
    public double dot(Vector vec) {
        if (vec.size() != sto.size())
            throw new CardinalityException(sto.size(), vec.size());

        double sum = 0.0;
        int len = sto.size();

        for (int i = 0; i < len; i++)
            sum += sto.get(i) * vec.getX(i);

        return sum;
    }

    @Override
    public double getLengthSquared() {
        return dotSelf(); // TODO: need to cache for performance.
    }

    @Override
    public double getDistanceSquared(Vector vec) {
        if (vec.size() != sto.size())
            throw new CardinalityException(size(), vec.size());

        double thisLenSq = getLengthSquared();
        double thatLenSq = vec.getLengthSquared();
        double dot = dot(vec);
        double distEst = thisLenSq + thatLenSq - 2 * dot;

        if (distEst > 1.0e-3 * (thisLenSq + thatLenSq))
            // The vectors are far enough from each other that the formula is accurate.
            return Math.max(distEst, 0);
        else
            return foldMap(vec, Functions.PLUS, Functions.MINUS_SQUARED);
    }

    /**
     *
     * @return
     */
    protected double dotSelf() {
        double sum = 0.0;
        int len = sto.size();

        for (int i = 0; i < len; i++) {
            double v = sto.get(i);

            sum += v * v;
        }

        return sum;
    }

    @Override
    public Element getElement(int idx) {
        return mkElement(idx);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(sto);
        out.writeObject(guid);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        sto = (VectorStorage)in.readObject();
        guid = (IgniteUuid)in.readObject();
    }
}
