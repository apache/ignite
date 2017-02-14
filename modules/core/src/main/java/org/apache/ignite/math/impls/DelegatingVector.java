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
import java.io.*;
import java.util.*;
import java.util.function.*;

/**
 * Convenient class that can be used to add decorations to an existing vector. Subclasses
 * can add weights, indices, etc. while maintaining full vector functionality.
 */
public class DelegatingVector implements org.apache.ignite.math.Vector {
    // Delegating vector.
    private org.apache.ignite.math.Vector dlg;

    // Meta attribute storage.
    private Map<String, Object> meta = new HashMap<>();

    // GUID.
    private IgniteUuid guid = IgniteUuid.randomUuid();

    /**
     *
     * @param dlg
     */
    public DelegatingVector(org.apache.ignite.math.Vector dlg) {
        this.dlg = dlg;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(dlg);
        out.writeObject(meta);
        out.writeObject(guid);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        dlg = (org.apache.ignite.math.Vector)in.readObject();
        meta = (Map<String, Object>)in.readObject();
        guid = (IgniteUuid)in.readObject();
    }

    @Override
    public Map<String, Object> getMetaStorage() {
        return meta;
    }

    @Override
    public Matrix likeMatrix(int rows, int cols) {
        return dlg.likeMatrix(rows, cols);
    }

    @Override
    public Matrix toMatrix() {
        return dlg.toMatrix();
    }

    @Override
    public Matrix toMatrixPlusOne(double zeroVal) {
        return dlg.toMatrixPlusOne(zeroVal);
    }

    @Override
    public int size() {
        return dlg.size();
    }

    @Override
    public boolean isDense() {
        return dlg.isDense();
    }

    @Override
    public boolean isSequentialAccess() {
        return dlg.isSequentialAccess();
    }

    @Override
    public boolean isArrayBased() {
        return dlg.isArrayBased();
    }

    @Override
    public org.apache.ignite.math.Vector copy() {
        return new DelegatingVector(dlg);
    }

    @Override
    public Iterable<Element> all() {
        return dlg.all();
    }

    @Override
    public Iterable<Element> nonZeroes() {
        return dlg.nonZeroes();
    }

    @Override
    public Spliterator<Double> allSpliterator() {
        return dlg.allSpliterator();
    }

    @Override
    public Spliterator<Double> nonZeroSpliterator() {
        return dlg.nonZeroSpliterator();
    }

    @Override
    public Element getElement(int idx) {
        return dlg.getElement(idx);
    }

    @Override
    public org.apache.ignite.math.Vector assign(double val) {
        return dlg.assign(val);
    }

    @Override
    public org.apache.ignite.math.Vector assign(double[] vals) {
        return dlg.assign(vals);
    }

    @Override
    public org.apache.ignite.math.Vector assign(org.apache.ignite.math.Vector vec) {
        return dlg.assign(vec);
    }

    @Override
    public org.apache.ignite.math.Vector assign(IntToDoubleFunction fun) {
        return dlg.assign(fun);
    }

    @Override
    public org.apache.ignite.math.Vector map(DoubleFunction<Double> fun) {
        return dlg.map(fun);
    }

    @Override
    public org.apache.ignite.math.Vector map(org.apache.ignite.math.Vector vec, BiFunction<Double, Double, Double> fun) {
        return dlg.map(vec, fun);
    }

    @Override
    public org.apache.ignite.math.Vector map(BiFunction<Double, Double, Double> fun, double y) {
        return dlg.map(fun, y);
    }

    @Override
    public org.apache.ignite.math.Vector divide(double x) {
        return dlg.divide(x);
    }

    @Override
    public double dot(org.apache.ignite.math.Vector vec) {
        return dlg.dot(vec);
    }

    @Override
    public double get(int idx) {
        return dlg.get(idx);
    }

    @Override
    public double getX(int idx) {
        return dlg.getX(idx);
    }

    @Override
    public org.apache.ignite.math.Vector like(int crd) {
        return dlg.like(crd);
    }

    @Override
    public org.apache.ignite.math.Vector minus(org.apache.ignite.math.Vector vec) {
        return dlg.minus(vec);
    }

    @Override
    public org.apache.ignite.math.Vector normalize() {
        return dlg.normalize();
    }

    @Override
    public org.apache.ignite.math.Vector normalize(double power) {
        return dlg.normalize(power);
    }

    @Override
    public org.apache.ignite.math.Vector logNormalize() {
        return dlg.logNormalize();
    }

    @Override
    public org.apache.ignite.math.Vector logNormalize(double power) {
        return dlg.logNormalize(power);
    }

    @Override
    public double kNorm(double power) {
        return dlg.kNorm(power);
    }

    @Override
    public Element minValue() {
        return dlg.minValue();
    }

    @Override
    public Element maxValue() {
        return dlg.maxValue();
    }

    @Override
    public org.apache.ignite.math.Vector plus(double x) {
        return dlg.plus(x);
    }

    @Override
    public org.apache.ignite.math.Vector plus(org.apache.ignite.math.Vector vec) {
        return dlg.plus(vec);
    }

    @Override
    public org.apache.ignite.math.Vector set(int idx, double val) {
        return dlg.set(idx, val);
    }

    @Override
    public org.apache.ignite.math.Vector setX(int idx, double val) {
        return dlg.setX(idx, val);
    }

    @Override
    public org.apache.ignite.math.Vector incrementX(int idx, double val) {
        return dlg.incrementX(idx, val);
    }

    @Override
    public org.apache.ignite.math.Vector increment(int idx, double val) {
        return dlg.increment(idx, val);
    }

    @Override
    public int nonZeroElements() {
        return dlg.nonZeroElements();
    }

    @Override
    public org.apache.ignite.math.Vector times(double x) {
        return dlg.times(x);
    }

    @Override
    public org.apache.ignite.math.Vector times(org.apache.ignite.math.Vector vec) {
        return dlg.times(vec);
    }

    @Override
    public org.apache.ignite.math.Vector viewPart(int off, int len) {
        return dlg.viewPart(off, len);
    }

    @Override
    public VectorStorage getStorage() {
        return dlg.getStorage();
    }

    @Override
    public double sum() {
        return dlg.sum();
    }

    @Override
    public Matrix cross(org.apache.ignite.math.Vector vec) {
        return dlg.cross(vec);
    }

    @Override
    public <T> T foldMap(BiFunction<T, Double, T> foldFun, DoubleFunction<Double> mapFun, T zeroVal) {
        return dlg.foldMap(foldFun, mapFun, zeroVal);
    }

    @Override
    public <T> T foldMap(org.apache.ignite.math.Vector vec, BiFunction<T, Double, T> foldFun, BiFunction<Double, Double, Double> combFun, T zeroVal) {
        return dlg.foldMap(vec, foldFun, combFun, zeroVal);
    }

    @Override
    public double getLengthSquared() {
        return dlg.getLengthSquared();
    }

    @Override
    public double getDistanceSquared(org.apache.ignite.math.Vector vec) {
        return dlg.getDistanceSquared(vec);
    }

    @Override
    public double getLookupCost() {
        return dlg.getLookupCost();
    }

    @Override
    public boolean isAddConstantTime() {
        return dlg.isAddConstantTime();
    }

    @Override
    public Optional<ClusterGroup> clusterGroup() {
        return dlg.clusterGroup();
    }

    @Override
    public IgniteUuid guid() {
        return guid;
    }
}
