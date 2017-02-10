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

package org.apache.ignite.math;

import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import java.util.*;
import java.util.function.*;

/**
 * Convenient class that can be used to add decorations to an existing vector. Subclasses
 * can add weights, indices, etc. while maintaining full vector functionality.
 */
public class DelegatingVector implements Vector {
    // Delegating vector.
    private Vector dlg;

    // GUID.
    private final IgniteUuid guid = IgniteUuid.randomUuid();

    /**
     *
     * @param dlg
     */
    public DelegatingVector(Vector dlg) {
        this.dlg = dlg;
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
    public Vector copy() {
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
    public Vector assign(double val) {
        return dlg.assign(val);
    }

    @Override
    public Vector assign(double[] vals) {
        return dlg.assign(vals);
    }

    @Override
    public Vector assign(Vector vec) {
        return dlg.assign(vec);
    }

    @Override
    public Vector assign(IntToDoubleFunction fun) {
        return dlg.assign(fun);
    }

    @Override
    public Vector map(DoubleFunction<Double> fun) {
        return dlg.map(fun);
    }

    @Override
    public Vector map(Vector vec, BiFunction<Double, Double, Double> fun) {
        return dlg.map(vec, fun);
    }

    @Override
    public Vector map(BiFunction<Double, Double, Double> fun, double y) {
        return dlg.map(fun, y);
    }

    @Override
    public Vector divide(double x) {
        return dlg.divide(x);
    }

    @Override
    public double dot(Vector vec) {
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
    public Vector like(int crd) {
        return dlg.like(crd);
    }

    @Override
    public Vector minus(Vector vec) {
        return dlg.minus(vec);
    }

    @Override
    public Vector normalize() {
        return dlg.normalize();
    }

    @Override
    public Vector normalize(double power) {
        return dlg.normalize(power);
    }

    @Override
    public Vector logNormalize() {
        return dlg.logNormalize();
    }

    @Override
    public Vector logNormalize(double power) {
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
    public Vector plus(double x) {
        return dlg.plus(x);
    }

    @Override
    public Vector plus(Vector vec) {
        return dlg.plus(vec);
    }

    @Override
    public Vector set(int idx, double val) {
        return dlg.set(idx, val);
    }

    @Override
    public Vector setX(int idx, double val) {
        return dlg.setX(idx, val);
    }

    @Override
    public Vector incrementX(int idx, double val) {
        return dlg.incrementX(idx, val);
    }

    @Override
    public Vector increment(int idx, double val) {
        return dlg.increment(idx, val);
    }

    @Override
    public int nonZeroElements() {
        return dlg.nonZeroElements();
    }

    @Override
    public Vector times(double x) {
        return dlg.times(x);
    }

    @Override
    public Vector times(Vector vec) {
        return dlg.times(vec);
    }

    @Override
    public Vector viewPart(int off, int len) {
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
    public Matrix cross(Vector vec) {
        return dlg.cross(vec);
    }

    @Override
    public <T> T foldMap(BiFunction<T, Double, T> foldFun, DoubleFunction<Double> mapFun, T zeroVal) {
        return dlg.foldMap(foldFun, mapFun, zeroVal);
    }

    @Override
    public <T> T foldMap(Vector vec, BiFunction<T, Double, T> foldFun, BiFunction<Double, Double, Double> combFun, T zeroVal) {
        return dlg.foldMap(vec, foldFun, combFun, zeroVal);
    }

    @Override
    public double getLengthSquared() {
        return dlg.getLengthSquared();
    }

    @Override
    public double getDistanceSquared(Vector vec) {
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
