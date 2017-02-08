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
import org.apache.ignite.math.impls.storage.*;
import java.io.*;
import java.util.*;
import java.util.function.*;

/**
 * TODO: add description.
 */
public abstract class AbstractVector implements Vector, Externalizable {
    /** Vector storage implementation. */
    private VectorStorage sto;

    /** Vector's GUID. */
    private IgniteUuid guid = IgniteUuid.randomUuid();

    /** Cached value for length squared. */
    private double lenSq = 0.0;

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
        sto = new VectorNullStorage();
    }

    /**
     *
     * @param sto
     */
    protected void setStorage(VectorStorage sto) {
        this.sto = sto == null ? new VectorNullStorage() : sto;
    }

    /**
     *
     * @param i
     * @param v
     */
    protected void storageSet(int i, double v) {
        sto.set(i, v);

        lenSq = 0.0;
    }

    /**
     *
     * @param i
     * @return
     */
    protected double storageGet(int i) {
        return sto.get(i);
    }

    /** {@inheritDoc */
    @Override public int size() {
        return sto == null ? 0 : sto.size();
    }

    /**
     * check index bounds
     *
     * @param idx index
     */
    private void checkIndex(int idx) {
        if (idx < 0 || idx >= sto.size())
            throw new IndexException(idx);
    }

    /**
     * {@inheritDoc}
     */
    @Override public double get(int idx) {
        checkIndex(idx);

        return storageGet(idx);
    }

    /** {@inheritDoc */
    @Override public double getX(int idx) {
        return storageGet(idx);
    }

    /** {@inheritDoc */
    @Override public Vector map(DoubleFunction<Double> fun) {
        if (sto.isArrayBased()) {
            double[] data = sto.data();

            Arrays.setAll(data, (idx) -> fun.apply(data[idx]));
        }
        else {
            int len = sto.size();

            for (int i = 0; i < len; i++)
                storageSet(i, fun.apply(storageGet(i)));
        }

        return this;
    }

    /** {@inheritDoc */
    @Override public Vector map(Vector vec, BiFunction<Double, Double, Double> fun) {
        checkCardinality(vec);

        int len = sto.size();

        for (int i = 0; i < len; i++)
            storageSet(i, fun.apply(vec.get(i), storageGet(i)));

        return this;
    }

    /** {@inheritDoc */
    @Override public Vector map(BiFunction<Double, Double, Double> fun, double y) {
        int len = sto.size();

        for (int i = 0; i < len; i++)
            storageSet(i, fun.apply(storageGet(i), y));

        return this;
    }

    /**
     *
     * @param idx
     * @return
     */
    private Element mkElement(int idx) {
        return new Element() {
            /** {@inheritDoc */
            @Override public double get() {
                return storageGet(idx);
            }

            /** {@inheritDoc */
            @Override public int index() {
                return idx;
            }

            /** {@inheritDoc */
            @Override public void set(double val) {
                storageSet(idx, val);
            }
        };
    }

    /** {@inheritDoc */
    @Override public Element minValue() {
        int minIdx = 0;
        int len = sto.size();

        for (int i = 0; i < len; i++)
            if (storageGet(i) < storageGet(minIdx))
                minIdx = i;

        return mkElement(minIdx);
    }

    /** {@inheritDoc */
    @Override public Element maxValue() {
        int maxIdx = 0;
        int len = sto.size();

        for (int i = 0; i < len; i++)
            if (storageGet(i) > storageGet(maxIdx))
                maxIdx = i;

        return mkElement(maxIdx);
    }

    /** {@inheritDoc */
    @Override public Vector set(int idx, double val) {
        checkIndex(idx);

        storageSet(idx, val);

        return this;
    }

    /** {@inheritDoc */
    @Override public Vector setX(int idx, double val) {
        storageSet(idx, val);

        return this;
    }

    /** {@inheritDoc */
    @Override public Vector increment(int idx, double val) {
        checkIndex(idx);

        storageSet(idx, storageGet(idx) + val);

        return this;
    }

    /** {@inheritDoc */
    @Override public Vector incrementX(int idx, double val) {
        storageSet(idx, storageGet(idx) + val);

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

    /** {@inheritDoc */
    @Override public double sum() {
        double sum = 0;
        int len = sto.size();

        for (int i = 0; i < len; i++)
            sum += storageGet(i);

        return sum;
    }

    /** {@inheritDoc */
    @Override public IgniteUuid guid() {
        return guid;
    }

    /** {@inheritDoc */
    @Override public int hashCode() {
        return guid.hashCode();
    }

    /** {@inheritDoc */
    @Override public boolean equals(Object o) {
        return this == o || o != null && ((getClass() == o.getClass())) && ((sto.equals(((AbstractVector)o).sto)));
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
    public <T> T foldMap(BiFunction<T, Double, T> foldFun, DoubleFunction<Double> mapFun, T zeroVal) {
        T result = zeroVal;
        int len = sto.size();

        for (int i = 0; i < len; i++)
            result = foldFun.apply(result, mapFun.apply(storageGet(i)));

        return result;
    }

    @Override
    public <T> T foldMap(Vector vec, BiFunction<T, Double, T> foldFun, BiFunction<Double, Double, Double> combFun, T zeroVal) {
        checkCardinality(vec);

        T result = zeroVal;
        int len = sto.size();

        for (int i = 0; i < len; i++)
            result = foldFun.apply(result, combFun.apply(storageGet(i), vec.getX(i)));

        return result;
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

                        if (idxNextInitialized() && idx != idxNext)
                            return;

                        if (idxNextInitialized())
                            idx = idxNext + 1;

                        while (idx < size() && isZero(get(idx)))
                            idx++;

                        idxNext = idx++;
                    }

                    private boolean over() {
                        return idxNext >= size();
                    }

                    private boolean idxNextInitialized() {
                        return idxNext != -1;
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
                storageSet(i, val);
        }

        return this;
    }

    @Override
    public Vector assign(double[] vals) {
        checkCardinality(vals);

        if (sto.isArrayBased())
            System.arraycopy(vals, 0, sto.data(), 0, vals.length);
        else {
            int len = sto.size();

            for (int i = 0; i < len; i++)
                storageSet(i, vals[i]);
        }

        return this;
    }

    @Override
    public Vector assign(Vector vec) {
        checkCardinality(vec);

        for (Vector.Element x : vec.all())
            storageSet(x.index(), x.get());

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
                storageSet(i, fun.applyAsDouble(i));
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
                    action.accept(storageGet(i));

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
                    double val = storageGet(i);

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
        checkCardinality(vec);

        double sum = 0.0;
        int len = sto.size();

        for (int i = 0; i < len; i++)
            sum += storageGet(i) * vec.getX(i);

        return sum;
    }

    @Override
    public double getLengthSquared() {
        if (lenSq == 0.0)
            lenSq = dotSelf();

        return lenSq;
    }

    @Override
    public boolean isDense() {
        return sto.isDense();
    }

    @Override
    public boolean isSequentialAccess() {
        return sto.isSequentialAccess();
    }

    @Override
    public double getLookupCost() {
        return sto.getLookupCost();
    }

    @Override
    public boolean isAddConstantTime() {
        return sto.isAddConstantTime();
    }

    @Override
    public VectorStorage getStorage() {
        return sto;
    }

    @Override
    public Vector viewPart(int off, int len) {
        return new VectorView(this, off, len);
    }

    @Override
    public Matrix cross(Vector vec) {
        return null; // TODO
    }

    @Override
    public double getDistanceSquared(Vector vec) {
        checkCardinality(vec);

        double thisLenSq = getLengthSquared();
        double thatLenSq = vec.getLengthSquared();
        double dot = dot(vec);
        double distEst = thisLenSq + thatLenSq - 2 * dot;

        if (distEst > 1.0e-3 * (thisLenSq + thatLenSq))
            // The vectors are far enough from each other that the formula is accurate.
            return Math.max(distEst, 0);
        else
            return foldMap(vec, Functions.PLUS, Functions.MINUS_SQUARED, 0d);
    }

    private void checkCardinality(Vector vec) {
        if (vec.size() != sto.size())
            throw new CardinalityException(size(), vec.size());
    }

    private void checkCardinality(double[] vec) {
        if (vec.length != sto.size())
            throw new CardinalityException(size(), vec.length);
    }

    @Override
    public Vector minus(Vector vec) {
        checkCardinality(vec);

        Vector copy = copy();

        copy.map(vec, Functions.MINUS);

        return copy;
    }

    @Override
    public Vector plus(double x) {
        Vector copy = copy();

        if (x != 0.0)
            copy.map(Functions.plus(x));

        return copy;
    }

    @Override
    public Vector divide(double x) {
        Vector copy = copy();

        if (x != 1.0)
            for (Element element : copy.nonZeroes())
                element.set(element.get() / x);

        return copy;
    }

    @Override
    public Vector times(double x) {
        if (x == 0.0)
            return like(size());
        else
            return copy().map(Functions.mult(x));
    }

    @Override
    public Vector times(Vector vec) {
        checkCardinality(vec);

        return copy().map(vec, Functions.MULT);
    }

    @Override
    public Vector plus(Vector vec) {
        checkCardinality(vec);

        Vector copy = copy();

        copy.map(vec, Functions.PLUS);

        return copy;
    }

    @Override
    public Vector logNormalize() {
        return logNormalize(2.0, Math.sqrt(getLengthSquared()));
    }

    @Override
    public Vector logNormalize(double power) {
        return logNormalize(power, kNorm(power));
    }

    /**
     *
     * @param power
     * @param normLen
     * @return
     */
    private Vector logNormalize(double power, double normLen) {
        assert Double.isInfinite(power) || power <= 1.0;

        double denominator = normLen * Math.log(power);

        Vector copy = copy();

        for (Element element : copy.nonZeroes())
            element.set(Math.log1p(element.get()) / denominator);

        return copy;
    }

    @Override
    public double kNorm(double power) {
        assert power >= 0.0;

        // Special cases.
        if (Double.isInfinite(power))
            return foldMap(Math::max, Math::abs, 0d);
        else if (power == 2.0)
            return Math.sqrt(getLengthSquared());
        else if (power == 1.0)
            return foldMap(Functions.PLUS, Math::abs, 0d);
        else if (power == 0.0)
            return nonZeroElements();
        else
            // Default case.
            return Math.pow(foldMap(Functions.PLUS, Functions.pow(power), 0d), 1.0 / power);
    }

    @Override
    public Vector normalize() {
        return divide(Math.sqrt(getLengthSquared()));
    }

    @Override
    public Vector normalize(double power) {
        return divide(kNorm(power));
    }

    /**
     *
     * @return
     */
    protected double dotSelf() {
        double sum = 0.0;
        int len = sto.size();

        for (int i = 0; i < len; i++) {
            double v = storageGet(i);

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
