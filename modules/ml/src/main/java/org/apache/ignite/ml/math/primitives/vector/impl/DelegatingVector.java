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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.IntToDoubleFunction;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteIntDoubleToDoubleBiFunction;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorStorage;

/**
 * Convenient class that can be used to add decorations to an existing vector. Subclasses
 * can add weights, indices, etc. while maintaining full vector functionality.
 */
public class DelegatingVector implements Vector {
    /** Delegating vector. */
    private Vector dlg;

    /** Meta attribute storage. */
    private Map<String, Object> meta = new HashMap<>();

    /** GUID. */
    private IgniteUuid guid = IgniteUuid.randomUuid();

    /** */
    public DelegatingVector() {
        // No-op.
    }

    /**
     * @param dlg Parent vector.
     */
    public DelegatingVector(Vector dlg) {
        assert dlg != null;

        this.dlg = dlg;
    }

    /** Get the delegating vector */
    public Vector getVector() {
        return dlg;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(dlg);
        out.writeObject(meta);
        out.writeObject(guid);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        dlg = (Vector)in.readObject();
        meta = (Map<String, Object>)in.readObject();
        guid = (IgniteUuid)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> getMetaStorage() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override public Matrix likeMatrix(int rows, int cols) {
        return dlg.likeMatrix(rows, cols);
    }

    /** {@inheritDoc} */
    @Override public Matrix toMatrix(boolean rowLike) {
        return dlg.toMatrix(rowLike);
    }

    /** {@inheritDoc} */
    @Override public Matrix toMatrixPlusOne(boolean rowLike, double zeroVal) {
        return dlg.toMatrixPlusOne(rowLike, zeroVal);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return dlg.size();
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return dlg.isDense();
    }

    /** {@inheritDoc} */
    @Override public double minValue() {
        return dlg.minValue();
    }

    /** {@inheritDoc} */
    @Override public double maxValue() {
        return dlg.maxValue();
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return dlg.isSequentialAccess();
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return dlg.isArrayBased();
    }

    /** {@inheritDoc} */
    @Override public Vector copy() {
        return new DelegatingVector(dlg);
    }

    /** {@inheritDoc} */
    @Override public Iterable<Element> all() {
        return dlg.all();
    }

    /** {@inheritDoc} */
    @Override public Iterable<Element> nonZeroes() {
        return dlg.nonZeroes();
    }

    /** {@inheritDoc} */
    @Override public Vector sort() {
        return dlg.sort();
    }

    /** {@inheritDoc} */
    @Override public Spliterator<Double> allSpliterator() {
        return dlg.allSpliterator();
    }

    /** {@inheritDoc} */
    @Override public Spliterator<Double> nonZeroSpliterator() {
        return dlg.nonZeroSpliterator();
    }

    /** {@inheritDoc} */
    @Override public Element getElement(int idx) {
        return dlg.getElement(idx);
    }

    /** {@inheritDoc} */
    @Override public Vector assign(double val) {
        return dlg.assign(val);
    }

    /** {@inheritDoc} */
    @Override public Vector assign(double[] vals) {
        return dlg.assign(vals);
    }

    /** {@inheritDoc} */
    @Override public Vector assign(Vector vec) {
        return dlg.assign(vec);
    }

    /** {@inheritDoc} */
    @Override public Vector assign(IntToDoubleFunction fun) {
        return dlg.assign(fun);
    }

    /** {@inheritDoc} */
    @Override public Vector map(IgniteDoubleFunction<Double> fun) {
        return dlg.map(fun);
    }

    /** {@inheritDoc} */
    @Override public Vector map(Vector vec, IgniteBiFunction<Double, Double, Double> fun) {
        return dlg.map(vec, fun);
    }

    /** {@inheritDoc} */
    @Override public Vector map(IgniteBiFunction<Double, Double, Double> fun, double y) {
        return dlg.map(fun, y);
    }

    /** {@inheritDoc} */
    @Override public Vector divide(double x) {
        return dlg.divide(x);
    }

    /** {@inheritDoc} */
    @Override public double dot(Vector vec) {
        return dlg.dot(vec);
    }

    /** {@inheritDoc} */
    @Override public double get(int idx) {
        return dlg.get(idx);
    }

    /** {@inheritDoc} */
    @Override public double getX(int idx) {
        return dlg.getX(idx);
    }

    /** {@inheritDoc} */
    @Override public Vector like(int crd) {
        return dlg.like(crd);
    }

    /** {@inheritDoc} */
    @Override public Vector minus(Vector vec) {
        return dlg.minus(vec);
    }

    /** {@inheritDoc} */
    @Override public Vector normalize() {
        return dlg.normalize();
    }

    /** {@inheritDoc} */
    @Override public Vector normalize(double power) {
        return dlg.normalize(power);
    }

    /** {@inheritDoc} */
    @Override public Vector logNormalize() {
        return dlg.logNormalize();
    }

    /** {@inheritDoc} */
    @Override public Vector logNormalize(double power) {
        return dlg.logNormalize(power);
    }

    /** {@inheritDoc} */
    @Override public double kNorm(double power) {
        return dlg.kNorm(power);
    }

    /** {@inheritDoc} */
    @Override public Element minElement() {
        return dlg.minElement();
    }

    /** {@inheritDoc} */
    @Override public Element maxElement() {
        return dlg.maxElement();
    }

    /** {@inheritDoc} */
    @Override public Vector plus(double x) {
        return dlg.plus(x);
    }

    /** {@inheritDoc} */
    @Override public Vector plus(Vector vec) {
        return dlg.plus(vec);
    }

    /** {@inheritDoc} */
    @Override public Vector set(int idx, double val) {
        return dlg.set(idx, val);
    }

    /** {@inheritDoc} */
    @Override public Vector setX(int idx, double val) {
        return dlg.setX(idx, val);
    }

    /** {@inheritDoc} */
    @Override public Vector incrementX(int idx, double val) {
        return dlg.incrementX(idx, val);
    }

    /** {@inheritDoc} */
    @Override public Vector increment(int idx, double val) {
        return dlg.increment(idx, val);
    }

    /** {@inheritDoc} */
    @Override public int nonZeroElements() {
        return dlg.nonZeroElements();
    }

    /** {@inheritDoc} */
    @Override public Vector times(double x) {
        return dlg.times(x);
    }

    /** {@inheritDoc} */
    @Override public Vector times(Vector vec) {
        return dlg.times(vec);
    }

    /** {@inheritDoc} */
    @Override public Vector viewPart(int off, int len) {
        return dlg.viewPart(off, len);
    }

    /** {@inheritDoc} */
    @Override public VectorStorage getStorage() {
        return dlg.getStorage();
    }

    /** {@inheritDoc} */
    @Override public double sum() {
        return dlg.sum();
    }

    /** {@inheritDoc} */
    @Override public Matrix cross(Vector vec) {
        return dlg.cross(vec);
    }

    /** {@inheritDoc} */
    @Override public <T> T foldMap(IgniteBiFunction<T, Double, T> foldFun, IgniteDoubleFunction<Double> mapFun,
        T zeroVal) {
        return dlg.foldMap(foldFun, mapFun, zeroVal);
    }

    /** {@inheritDoc} */
    @Override public <T> T foldMap(Vector vec, IgniteBiFunction<T, Double, T> foldFun,
        IgniteBiFunction<Double, Double, Double> combFun, T zeroVal) {
        return dlg.foldMap(vec, foldFun, combFun, zeroVal);
    }

    /** {@inheritDoc} */
    @Override public double getLengthSquared() {
        return dlg.getLengthSquared();
    }

    /** {@inheritDoc} */
    @Override public double getDistanceSquared(Vector vec) {
        return dlg.getDistanceSquared(vec);
    }

    /** {@inheritDoc} */
    @Override public boolean isRandomAccess() {
        return dlg.isRandomAccess();
    }

    /** {@inheritDoc} */
    @Override public boolean isDistributed() {
        return dlg.isDistributed();
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid guid() {
        return guid;
    }

    /** {@inheritDoc} */
    @Override public void compute(int i, IgniteIntDoubleToDoubleBiFunction f) {
        dlg.compute(i, f);
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        dlg.destroy();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + meta.hashCode();
        res = res * 37 + dlg.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        DelegatingVector that = (DelegatingVector)o;

        return meta.equals(that.meta) && dlg.equals(that.dlg);
    }
}
