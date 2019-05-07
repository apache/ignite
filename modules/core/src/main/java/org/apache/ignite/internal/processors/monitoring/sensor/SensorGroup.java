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

package org.apache.ignite.internal.processors.monitoring.sensor;

import java.util.Collection;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.ignite.internal.processors.monitoring.MonitoringGroup;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface SensorGroup {
    /** */
    public MonitoringGroup getGroup();

    /** */
    public String getName();

    /** */
    public Collection<Sensor> getSensors();

    /** */
    @Nullable public Sensor findSensor(String name);

    /** */
    public <T> TypedSensor<T> sensor(String name);

    /** */
    public <T> TypedSensor<T> sensor(String name, T value);

    /** */
    public <T> ClosureSensor<T> sensor(String name, Supplier<T> value);

    /** */
    public LongSensor longSensor(String name);

    /** */
    public LongSensor longSensor(String name, long value);

    /** */
    public LongAdderSensor longAdderSensor(String name);

    /** */
    public AtomicLongSensor atomicLongSensor(String name);

    /** */
    public DoubleConcurrentSensor doubleConcurrentSensor(String name);

    /** */
    public DoubleConcurrentSensor doubleConcurrentSensor(String name, DoubleToDoubleFunction valueProducer);

    /** */
    public DoubleSensor doubleSensor(String name);

    /** */
    public DoubleSensor doubleSensor(String name, double value);

    /** */
    public LongClosureSensor longSensor(String name, LongSupplier sensorValue);

    /** */
    public DoubleClosureSensor doubleSensor(String name, DoubleSupplier sensorValue);

    /** */
    public FloatClosureSensor floatSensor(String name, FloatSupplier sensorValue);

    /** */
    public HitRateSensor hitRateSensor(String name, int rateTimeInterval, int size);

    /** */
    public BooleanClosureSensor booleanSensor(String name, BooleanSupplier sensorValue);

    /** */
    public LongArraySensor longArraySensor(String name, Supplier<long[]> sensorValue);

    /** */
    public BooleanArraySensor booleanArraySensor(String name, Supplier<boolean[]> sensorValue);

    /** */
    public IntArraySensor intArraySensor(String name, Supplier<int[]> sensorValue);

    /** */
    public IntSensor intSensor(String name);

    /** */
    @FunctionalInterface
    public interface DoubleToDoubleFunction {
        /** */
        public double apply(double value);
    }

    /** */
    @FunctionalInterface
    public interface FloatSupplier {

        /**
         * Gets a result.
         *
         * @return a result.
         */
        float getAsFloat();
    }
}
