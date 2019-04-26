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
import java.util.concurrent.Callable;

/**
 *
 */
public class SensorGroupImpl<Name> implements SensorGroup<Name> {
    Name name;

    private Map<String, Sensor> sensors = new HashMap<>();

    public SensorGroupImpl(Name name) {
        this.name = name;
    }

    @Override public LongSensor longSensor(String name) {
        return longSensor(name, 0);
    }

    @Override public LongSensor longSensor(String name, long value) {
        LongSensor sensor = new LongSensor(name, value);

        Sensor old = sensors.putIfAbsent(name, sensor);

        assert old == null;

        return sensor;
    }

    @Override public DoubleSensor doubleSensor(String name) {
        return doubleSensor(name, 0);
    }

    @Override public DoubleSensor doubleSensor(String name, double value) {
        DoubleSensor sensor = new DoubleSensor(name, value);

        Sensor old = sensors.putIfAbsent(name, sensor);

        assert old == null;

        return sensor;
    }

    @Override public LongClosureSensor longSensor(String name, LongClosureSensor.LongClosure sensorValue) {
        LongClosureSensor sensor = new LongClosureSensor(name, sensorValue);

        Sensor old = sensors.putIfAbsent(name, sensor);

        assert old == null;

        return sensor;
    }

    @Override public DoubleClosureSensor doubleSensor(String name, DoubleClosureSensor.DoubleClosure sensorValue) {
        DoubleClosureSensor sensor = new DoubleClosureSensor(name, sensorValue);

        Sensor old = sensors.putIfAbsent(name, sensor);

        assert old == null;

        return sensor;
    }

    @Override public <T> TypedSensor<T> sensor(String name, T value) {
        TypedSensor<T> sensor = new TypedSensor<>(name);

        sensor.setValue(value);

        Sensor old = sensors.putIfAbsent(name, sensor);

        assert old == null;

        return sensor;
    }

    @Override public <T> ClosureSensor<T> sensor(String name, Callable<T> value) {
            ClosureSensor<T> sensor = new ClosureSensor<>(name, value);

            Sensor old = sensors.putIfAbsent(name, sensor);

            assert old == null;

            return sensor;
    }

    @Override public <T> TypedSensor<T> sensor(String name) {
        return sensor(name, (T) null);
    }

    @Override public Name getName() {
        return name;
    }

    @Override public Collection<Sensor> getSensors() {
        return sensors.values();
    }
}
