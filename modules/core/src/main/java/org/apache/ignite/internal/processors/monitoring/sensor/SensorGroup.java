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
import java.util.concurrent.Callable;

/**
 *
 */
public interface SensorGroup<Name> {
    public Name getName();

    public Collection<Sensor> getSensors();

    public <T> TypedSensor<T> sensor(String name);

    public <T> TypedSensor<T> sensor(String name, T value);

    public <T> ClosureSensor<T> sensor(String name, Callable<T> value);

    public LongSensor longSensor(String name);

    public LongSensor longSensor(String name, long value);

    public DoubleSensor doubleSensor(String name);

    public DoubleSensor doubleSensor(String name, double value);

    public LongClosureSensor longSensor(String name, LongClosureSensor.LongClosure sensorValue);

    public DoubleClosureSensor doubleSensor(String name, DoubleClosureSensor.DoubleClosure sensorValue);
}
