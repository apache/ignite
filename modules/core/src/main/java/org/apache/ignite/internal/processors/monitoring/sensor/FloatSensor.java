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

/**
 *
 */
public class FloatSensor extends AbstractSensor {
    private float value;

    public FloatSensor(String name, float value) {
        super(name);

        this.value = value;
    }

    public float getValue() {
        return value;
    }

    /** {@inheritDoc} */
    @Override public String stringValue() {
        return ((Float) value).toString();
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        value = 0;
    }

    public void set(float value) {
        this.value = value;
    }
}
