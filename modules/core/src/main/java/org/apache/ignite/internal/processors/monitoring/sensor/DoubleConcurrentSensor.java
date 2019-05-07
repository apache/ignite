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

import java.util.concurrent.atomic.DoubleAdder;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class DoubleConcurrentSensor extends AbstractSensor {
    /** */
    private DoubleAdder value;

    private @Nullable SensorGroup.DoubleToDoubleFunction valueProducer;

    public DoubleConcurrentSensor(String name, @Nullable SensorGroup.DoubleToDoubleFunction valueProducer) {
        super(name);

        this.value = new DoubleAdder();
        this.valueProducer = valueProducer;
    }

    /** */
    public DoubleConcurrentSensor(String name) {
        this(name, null);
    }

    /** */
    public double getValue() {
        return value.doubleValue();
    }

    /** */
    public void increment() {
        value.add(1);
    }

    /** */
    public void decrement() {
        value.add(-1);
    }

    /** */
    public void add(double x) {
        value.add(x);
    }

    /** {@inheritDoc} */
    @Override public String stringValue() {
        if (valueProducer == null)
            return value.toString();
        else
            return ((Double)valueProducer.apply(value.doubleValue())).toString();
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        value.reset();
    }
}
