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
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.ignite.internal.processors.monitoring.MonitoringGroup;

/**
 *
 */
public class SensorGroupImpl implements SensorGroup {
    /** */
    private MonitoringGroup monGrp;

    /** */
    private String name;

    /** */
    private Map<String, Sensor> sensors = new HashMap<>();

    /** */
    public SensorGroupImpl(MonitoringGroup monGrp, String name) {
        this.monGrp = monGrp;

        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public LongSensor longSensor(String name) {
        return longSensor(name, 0);
    }

    /** {@inheritDoc} */
    @Override public LongSensor longSensor(String name, long value) {
        return addSensor(name, new LongSensor(name, value));
    }

    /** {@inheritDoc} */
    @Override public LongAdderSensor longAdderSensor(String name) {
        return addSensor(name, new LongAdderSensor(name));
    }

    /** {@inheritDoc} */
    @Override public AtomicLongSensor atomicLongSensor(String name) {
        return addSensor(name, new AtomicLongSensor(name));
    }

    @Override public DoubleConcurrentSensor doubleConcurrentSensor(String name) {
        return addSensor(name, new DoubleConcurrentSensor(name));
    }

    @Override public DoubleConcurrentSensor doubleConcurrentSensor(String name, DoubleToDoubleFunction valueProducer) {
        return addSensor(name, new DoubleConcurrentSensor(name, valueProducer));
    }

    /** {@inheritDoc} */
    @Override public DoubleSensor doubleSensor(String name) {
        return doubleSensor(name, 0);
    }

    /** {@inheritDoc} */
    @Override public DoubleSensor doubleSensor(String name, double value) {
        return addSensor(name, new DoubleSensor(name, value));
    }

    /** {@inheritDoc} */
    @Override public LongClosureSensor longSensor(String name, LongSupplier sensorValue) {
        return addSensor(name, new LongClosureSensor(name, sensorValue));
    }

    /** {@inheritDoc} */
    @Override public DoubleClosureSensor doubleSensor(String name, DoubleSupplier sensorValue) {
        return addSensor(name, new DoubleClosureSensor(name, sensorValue));
    }

    @Override public FloatClosureSensor floatSensor(String name, FloatSupplier sensorValue) {
        return addSensor(name, new FloatClosureSensor(name, sensorValue));
    }

    /** {@inheritDoc} */
    @Override public HitRateSensor hitRateSensor(String name, int rateTimeInterval, int size) {
        return addSensor(name, new HitRateSensor(name, rateTimeInterval, size));
    }

    @Override public BooleanClosureSensor booleanSensor(String name, BooleanSupplier sensorValue) {
        return addSensor(name, new BooleanClosureSensor(name, sensorValue));
    }

    @Override public LongArraySensor longArraySensor(String name, Supplier<long[]> sensorValue) {
        return addSensor(name, new LongArraySensor(name, sensorValue));
    }

    @Override public BooleanArraySensor booleanArraySensor(String name, Supplier<boolean[]> sensorValue) {
        return addSensor(name, new BooleanArraySensor(name, sensorValue));
    }

    @Override public IntArraySensor intArraySensor(String name, Supplier<int[]> sensorValue) {
        return addSensor(name, new IntArraySensor(name, sensorValue));
    }

    /** {@inheritDoc} */
    @Override public <T> TypedSensor<T> sensor(String name, T value) {
        return addSensor(name, new TypedSensor<>(name, value));
    }

    /** {@inheritDoc} */
    @Override public <T> ClosureSensor<T> sensor(String name, Supplier<T> value) {
        return addSensor(name, new ClosureSensor<>(name, value));
    }

    /** {@inheritDoc} */
    @Override public <T> TypedSensor<T> sensor(String name) {
        return sensor(name, (T) null);
    }

    @Override public MonitoringGroup getGroup() {
        return monGrp;
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public Collection<Sensor> getSensors() {
        return sensors.values();
    }

    /** {@inheritDoc} */
    @Override public Sensor findSensor(String name) {
        return sensors.get(name);
    }

    /** */
    private <T extends Sensor> T addSensor(String name, T sensor) {
        T old = (T)sensors.putIfAbsent(name, sensor);

        if (old == null)
            return sensor;

        return old;
    }
}
